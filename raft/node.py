"""
Complete Raft Consensus Node Implementation.

Satisfies CS G623 Milestone 2 requirements:
- Leader election with randomized election timer
- Heartbeats and failure detection
- AppendEntries log replication to majority quorum
- State machine commit and sequential application
- Transparent client request proposal and leader redirection
"""

import json
import logging
import random
import threading
import time
from concurrent import futures
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import grpc

import raft_pb2
import raft_pb2_grpc
from raft.log import LogEntry, RaftLog
from raft.state_machine import StateMachine

logger = logging.getLogger(__name__)


class NodeState(str, Enum):
    FOLLOWER  = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER    = "LEADER"


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    """
    Raft consensus engine.
    """

    def __init__(
        self,
        node_id: str,
        db,
        peers: Dict[str, str],  # node_id -> "host:port"
        state_machine: Optional[StateMachine] = None,
        heartbeat_interval: float = 0.05,  # 50ms
        min_election_timeout: float = 0.15,  # 150ms
        max_election_timeout: float = 0.35,  # 350ms
        peer_stubs: Optional[Dict[str, Any]] = None,
    ):
        self.node_id = str(node_id)
        self.db = db
        self.peers = dict(peers)  # All nodes in cluster (including self)
        self.log = RaftLog(db)
        self.state_machine = state_machine or StateMachine(db)

        self._lock = threading.RLock()

        # Persistent state on all servers
        self.current_term = self.log.get_current_term()
        self.voted_for = self.log.get_voted_for()

        # Volatile state on all servers
        self.commit_index = 0
        self.last_applied = self.state_machine.last_applied
        self.state = NodeState.FOLLOWER
        self.leader_id: Optional[str] = None

        # Volatile state on leaders
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Timings
        self.heartbeat_interval = heartbeat_interval
        self.min_election_timeout = min_election_timeout
        self.max_election_timeout = max_election_timeout
        self._last_heartbeat_time = time.time()
        self._election_timeout = self._random_timeout()

        # Proposal tracking: entry_index -> Event, result
        self._pending_events: Dict[int, threading.Event] = {}
        self._pending_results: Dict[int, Any] = {}

        # gRPC stubs to peers (can be injected for in-memory testing)
        self._peer_stubs: Dict[str, Any] = dict(peer_stubs or {})
        if not self._peer_stubs:
            self._init_peer_stubs()

        # Thread pools
        self._rpc_executor = futures.ThreadPoolExecutor(max_workers=16, thread_name_prefix="raft-rpc")

        # Background loops
        self._running = True
        self._apply_cv = threading.Condition(self._lock)

        self._ticker_thread = threading.Thread(target=self._run_ticker, daemon=True, name=f"raft-ticker-{self.node_id}")
        self._applier_thread = threading.Thread(target=self._run_applier, daemon=True, name=f"raft-applier-{self.node_id}")

        self._ticker_thread.start()
        self._applier_thread.start()

        # Single-node cluster fast-path
        if len(self.peers) <= 1:
            self._transition_to_leader()

        logger.info(
            "[RAFT] Node %s initialized (term=%d, peers=%s, state=%s)",
            self.node_id,
            self.current_term,
            list(self.peers.keys()),
            self.state.value,
        )

    def _random_timeout(self) -> float:
        return random.uniform(self.min_election_timeout, self.max_election_timeout)

    def _reset_election_timer(self):
        self._last_heartbeat_time = time.time()
        self._election_timeout = self._random_timeout()

    def _init_peer_stubs(self):
        for peer_id, addr in self.peers.items():
            if peer_id == self.node_id:
                continue
            channel = grpc.insecure_channel(
                addr,
                options=[
                    ("grpc.max_send_message_length", 64 * 1024 * 1024),
                    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
                ],
            )
            self._peer_stubs[peer_id] = raft_pb2_grpc.RaftServiceStub(channel)

    # ── State Transitions ─────────────────────────────────────────────────────

    def _transition_to_follower(self, term: int, leader_id: Optional[str] = None):
        self.state = NodeState.FOLLOWER
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.log.update_term_and_vote(term, None)
        self.leader_id = leader_id
        self._reset_election_timer()
        logger.info("[RAFT] Node %s -> FOLLOWER for term %d (leader=%s)", self.node_id, self.current_term, self.leader_id)

    def _transition_to_candidate(self):
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.log.update_term_and_vote(self.current_term, self.node_id)
        self.leader_id = None
        self._reset_election_timer()
        logger.info("[RAFT] Node %s -> CANDIDATE for term %d", self.node_id, self.current_term)

    def _transition_to_leader(self):
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        last_log_idx = self.log.last_index()

        for peer_id in self.peers:
            if peer_id != self.node_id:
                self.next_index[peer_id] = last_log_idx + 1
                self.match_index[peer_id] = 0

        logger.info("[RAFT] Node %s -> LEADER for term %d (last_log_index=%d)", self.node_id, self.current_term, last_log_idx)
        self._send_append_entries_all(is_heartbeat=True)

    # ── Background Loops ──────────────────────────────────────────────────────

    def _run_ticker(self):
        while self._running:
            time.sleep(0.02)  # Tick every 20ms
            with self._lock:
                now = time.time()
                if self.state == NodeState.LEADER:
                    if now - self._last_heartbeat_time >= self.heartbeat_interval:
                        self._last_heartbeat_time = now
                        self._send_append_entries_all(is_heartbeat=True)
                else:
                    if now - self._last_heartbeat_time >= self._election_timeout:
                        logger.info(
                            "[RAFT] Election timeout expired (%.3fs) on node %s",
                            self._election_timeout,
                            self.node_id,
                        )
                        self._start_election()

    def _run_applier(self):
        while self._running:
            with self._lock:
                while self.commit_index <= self.last_applied and self._running:
                    self._apply_cv.wait(timeout=0.1)

                if not self._running:
                    break

                to_apply = []
                while self.last_applied < self.commit_index:
                    self.last_applied += 1
                    entry = self.log.get(self.last_applied)
                    if entry:
                        to_apply.append(entry)

            # Apply entries sequentially outside the lock to prevent blocking Raft RPCs
            for entry in to_apply:
                res = self.state_machine.apply(entry)
                with self._lock:
                    if entry.index in self._pending_events:
                        self._pending_results[entry.index] = res
                        self._pending_events[entry.index].set()

    # ── Election Handling ─────────────────────────────────────────────────────

    def _start_election(self):
        self._transition_to_candidate()
        term = self.current_term
        last_log_idx = self.log.last_index()
        last_log_term = self.log.last_term()

        # Vote for self
        self._votes_received = set([self.node_id])
        votes = len(self._votes_received)
        total_nodes = len(self.peers)
        needed = (total_nodes // 2) + 1

        if votes >= needed:
            self._transition_to_leader()
            return

        for peer_id, stub in list(self._peer_stubs.items()):
            args = raft_pb2.RequestVoteArgs(
                **{
                    "from": self.node_id,
                    "to": peer_id,
                    "term": term,
                    "last_log_index": last_log_idx,
                    "last_log_term": last_log_term,
                }
            )
            if not self._running:
                return
            try:
                self._rpc_executor.submit(self._send_request_vote, peer_id, stub, args)
            except RuntimeError:
                pass

    def _send_request_vote(self, peer_id: str, stub, args):
        try:
            reply = stub.RequestVote(args, timeout=0.15)
            with self._lock:
                if self.state != NodeState.CANDIDATE or self.current_term != args.term:
                    return

                if reply.term > self.current_term:
                    self._transition_to_follower(reply.term)
                    return

                if reply.vote_granted and reply.term == self.current_term:
                    total_nodes = len(self.peers)
                    needed = (total_nodes // 2) + 1
                    if not hasattr(self, "_votes_received"):
                        self._votes_received = set([self.node_id])
                    self._votes_received.add(peer_id)

                    if len(self._votes_received) >= needed and self.state == NodeState.CANDIDATE:
                        logger.info(
                            "[RAFT] Node %s won election with %d/%d votes for term %d",
                            self.node_id,
                            len(self._votes_received),
                            total_nodes,
                            self.current_term,
                        )
                        self._transition_to_leader()
        except Exception as exc:
            logger.debug("[RAFT] RequestVote failed to peer %s: %s", peer_id, exc)

    # ── AppendEntries Replication ─────────────────────────────────────────────

    def _send_append_entries_all(self, is_heartbeat: bool = False):
        if self.state != NodeState.LEADER:
            return

        for peer_id, stub in list(self._peer_stubs.items()):
            prev_idx = self.next_index.get(peer_id, self.log.last_index() + 1) - 1
            prev_term = self.log.term_at(prev_idx)

            raw_entries = self.log.entries_from(prev_idx + 1)
            proto_entries = [
                raft_pb2.LogEntry(
                    index=e.index,
                    term=e.term,
                    command_type=e.command_type,
                    payload=json.dumps(e.payload) if isinstance(e.payload, dict) else str(e.payload),
                    request_id=e.request_id,
                )
                for e in raw_entries
            ]

            args = raft_pb2.AppendEntriesArgs(
                **{
                    "from": self.node_id,
                    "to": peer_id,
                    "term": self.current_term,
                    "prev_index": prev_idx,
                    "prev_term": prev_term,
                    "commit_index": self.commit_index,
                    "entries": proto_entries,
                }
            )
            if not self._running:
                return
            try:
                self._rpc_executor.submit(self._send_append_entries, peer_id, stub, args)
            except RuntimeError:
                pass

    def _send_append_entries(self, peer_id: str, stub, args):
        try:
            reply = stub.AppendEntries(args, timeout=0.15)
            with self._lock:
                if self.state != NodeState.LEADER or self.current_term != args.term:
                    return

                if reply.term > self.current_term:
                    self._transition_to_follower(reply.term)
                    return

                if reply.entry_appended:
                    self.match_index[peer_id] = reply.match_index
                    self.next_index[peer_id] = reply.match_index + 1
                    self._check_and_advance_commit_index()
                else:
                    # Decrement next_index on conflict and retry on next tick
                    self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)
        except Exception as exc:
            logger.debug("[RAFT] AppendEntries failed to peer %s: %s", peer_id, exc)

    def _check_and_advance_commit_index(self):
        """Advance commitIndex if majority has replicated entry from currentTerm."""
        if self.state != NodeState.LEADER:
            return

        total_nodes = len(self.peers)
        needed = (total_nodes // 2) + 1
        last_log_idx = self.log.last_index()

        for idx in range(last_log_idx, self.commit_index, -1):
            if self.log.term_at(idx) != self.current_term:
                continue

            # Count replicas (including self)
            count = 1
            for peer_id in self.peers:
                if peer_id != self.node_id and self.match_index.get(peer_id, 0) >= idx:
                    count += 1

            if count >= needed:
                logger.info(
                    "[RAFT] Advancing commit_index from %d to %d (quorum=%d/%d)",
                    self.commit_index,
                    idx,
                    count,
                    total_nodes,
                )
                self.commit_index = idx
                with self._apply_cv:
                    self._apply_cv.notify_all()
                # Immediately broadcast updated commit_index to followers
                self._send_append_entries_all(is_heartbeat=True)
                break

    # ── gRPC Handlers (Assignment Page 7 Signatures) ──────────────────────────

    def RequestVote(self, request, context):
        with self._lock:
            req_from = getattr(request, "from")
            # 1. Reply false if term < current_term
            if request.term < self.current_term:
                return raft_pb2.RequestVoteReply(
                    **{
                        "from": self.node_id,
                        "to": req_from,
                        "term": self.current_term,
                        "vote_granted": False,
                    }
                )

            # Step down if term > current_term
            if request.term > self.current_term:
                self._transition_to_follower(request.term)

            # 2. Check if we can vote for candidate
            can_vote = self.voted_for in (None, "", req_from)

            # Log is up-to-date check:
            last_term = self.log.last_term()
            last_idx = self.log.last_index()
            log_ok = (request.last_log_term > last_term) or (
                request.last_log_term == last_term and request.last_log_index >= last_idx
            )

            vote_granted = False
            if can_vote and log_ok:
                vote_granted = True
                self.voted_for = req_from
                self.log.set_voted_for(req_from)
                self._reset_election_timer()
                logger.info(
                    "[RAFT] Node %s voted FOR %s in term %d",
                    self.node_id,
                    req_from,
                    self.current_term,
                )
            else:
                logger.info(
                    "[RAFT] Node %s REJECTED vote for %s in term %d (can_vote=%s, log_ok=%s)",
                    self.node_id,
                    req_from,
                    self.current_term,
                    can_vote,
                    log_ok,
                )

            return raft_pb2.RequestVoteReply(
                **{
                    "from": self.node_id,
                    "to": req_from,
                    "term": self.current_term,
                    "vote_granted": vote_granted,
                }
            )

    def AppendEntries(self, request, context):
        with self._lock:
            req_from = getattr(request, "from")
            # 1. Reply false if term < current_term
            if request.term < self.current_term:
                return raft_pb2.AppendEntriesReply(
                    **{
                        "from": self.node_id,
                        "to": req_from,
                        "term": self.current_term,
                        "entry_appended": False,
                        "match_index": self.log.last_index(),
                    }
                )

            # Recognize valid leader & reset election timer
            if request.term > self.current_term or self.state != NodeState.FOLLOWER:
                self._transition_to_follower(request.term, leader_id=req_from)
            else:
                self.leader_id = req_from
                self._reset_election_timer()

            # 2. Reply false if log doesn't contain an entry at prev_index whose term matches prev_term
            if request.prev_index > 0:
                if self.log.last_index() < request.prev_index:
                    return raft_pb2.AppendEntriesReply(
                        **{
                            "from": self.node_id,
                            "to": req_from,
                            "term": self.current_term,
                            "entry_appended": False,
                            "match_index": self.log.last_index(),
                        }
                    )
                if self.log.term_at(request.prev_index) != request.prev_term:
                    self.log.truncate_from(request.prev_index)
                    return raft_pb2.AppendEntriesReply(
                        **{
                            "from": self.node_id,
                            "to": req_from,
                            "term": self.current_term,
                            "entry_appended": False,
                            "match_index": self.log.last_index(),
                        }
                    )

            # 3. If an existing entry conflicts with a new one, delete existing entry and following
            for e in request.entries:
                existing = self.log.get(e.index)
                if existing and existing.term != e.term:
                    self.log.truncate_from(e.index)
                # Append any new entries not already in the log
                self.log.append_entry(
                    LogEntry(
                        index=e.index,
                        term=e.term,
                        command_type=e.command_type,
                        payload=e.payload,
                        request_id=e.request_id,
                    )
                )

            # 4. If commit_index > self.commit_index, set commit_index = min(commit_index, index of last new entry)
            if request.commit_index > self.commit_index:
                self.commit_index = min(request.commit_index, self.log.last_index())
                with self._apply_cv:
                    self._apply_cv.notify_all()

            return raft_pb2.AppendEntriesReply(
                **{
                    "from": self.node_id,
                    "to": req_from,
                    "term": self.current_term,
                    "entry_appended": True,
                    "match_index": self.log.last_index(),
                }
            )

    # ── Client Proposal API ───────────────────────────────────────────────────

    def propose(self, command_type: str, payload: dict, request_id: str = "", timeout: float = 4.0) -> Tuple[bool, str, Optional[str], dict]:
        """
        Propose a state machine mutation to the Raft cluster.
        Returns: (success, error_message, leader_id, result_dict)
        """
        with self._lock:
            if self.state != NodeState.LEADER:
                return False, f"Not leader (leader={self.leader_id})", self.leader_id, {}

            # Append entry to local log
            entry = self.log.append(
                term=self.current_term,
                command_type=command_type,
                payload=payload,
                request_id=request_id,
            )
            event = threading.Event()
            self._pending_events[entry.index] = event

            # If single node, immediately advance commit
            if len(self.peers) <= 1:
                self.commit_index = entry.index
                with self._apply_cv:
                    self._apply_cv.notify_all()

        # Immediate broadcast to reduce latency
        self._send_append_entries_all()

        # Wait for entry to be applied to state machine
        signaled = event.wait(timeout=timeout)
        with self._lock:
            self._pending_events.pop(entry.index, None)
            res = self._pending_results.pop(entry.index, {})

        if not signaled:
            return False, f"Raft proposal timed out after {timeout}s", self.node_id, {}

        return True, "", self.node_id, res

    def status(self) -> dict:
        with self._lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "term": self.current_term,
                "leader_id": self.leader_id or "",
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
            }

    def stop(self):
        self._running = False
        with self._apply_cv:
            self._apply_cv.notify_all()
        try:
            self._ticker_thread.join(timeout=0.1)
        except Exception:
            pass
        try:
            self._applier_thread.join(timeout=0.1)
        except Exception:
            pass
        self._rpc_executor.shutdown(wait=False)
