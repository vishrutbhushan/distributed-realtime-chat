"""Compact Raft Consensus Engine with Dynamic Port-Range Peer Discovery."""

from concurrent import futures
from enum import Enum
import json
import logging
import os
import random
import threading
import time
from typing import Any, Dict, Optional, Tuple

import grpc
import raft_pb2
import raft_pb2_grpc
from raft.log import LogEntry, RaftLog
from raft.state_machine import StateMachine

logger = logging.getLogger(__name__)

_GRPC_OPTS = [("grpc.max_send_message_length", 64 * 1024 * 1024), ("grpc.max_receive_message_length", 64 * 1024 * 1024)]


class NodeState(str, Enum):
    FOLLOWER  = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER    = "LEADER"


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(
        self,
        node_id: str,
        db,
        peers: Dict[str, str],
        state_machine: Optional[StateMachine] = None,
        heartbeat_interval: float = 0.05,
        min_election_timeout: float = 0.15,
        max_election_timeout: float = 0.35,
        peer_stubs: Optional[Dict[str, Any]] = None,
        app_peers: Optional[Dict[str, str]] = None,
        self_raft_addr: str = "",
        self_app_addr: str = "",
    ):
        self.node_id = str(node_id)
        self.db = db
        self.peers = dict(peers)
        self.app_peers = dict(app_peers or {})
        self.self_raft_addr = self_raft_addr or self.peers.get(self.node_id, "")
        self.self_app_addr = self_app_addr or self.app_peers.get(self.node_id, "")
        self.log = RaftLog(db)
        self.state_machine = state_machine or StateMachine(db)

        self._lock = threading.RLock()
        self.current_term = self.log.get_current_term()
        self.voted_for = self.log.get_voted_for()
        self.commit_index = 0
        self.last_applied = self.state_machine.last_applied
        self.state = NodeState.FOLLOWER
        self.leader_id: Optional[str] = None
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self.heartbeat_interval = heartbeat_interval
        self.min_election_timeout = min_election_timeout
        self.max_election_timeout = max_election_timeout
        self._reset_election_timer()

        self._pending_events: Dict[int, threading.Event] = {}
        self._pending_results: Dict[int, Any] = {}
        self._peer_stubs: Dict[str, Any] = dict(peer_stubs or {})
        if not self._peer_stubs:
            for pid, addr in self.peers.items():
                if pid != self.node_id:
                    self._peer_stubs[pid] = raft_pb2_grpc.RaftServiceStub(grpc.insecure_channel(addr, options=_GRPC_OPTS))

        self._rpc_executor = futures.ThreadPoolExecutor(max_workers=16, thread_name_prefix="raft-rpc")
        self._running = True
        self._apply_cv = threading.Condition(self._lock)
        self._ticker_thread = threading.Thread(target=self._run_ticker, daemon=True)
        self._applier_thread = threading.Thread(target=self._run_applier, daemon=True)
        self._ticker_thread.start()
        self._applier_thread.start()

        if len(self.peers) <= 1 and not (
            os.environ.get("CLUSTER_DNS") or os.environ.get("RAFT_PORT_RANGE")
        ):
            self._become_leader()

    def _reset_election_timer(self):
        self._last_heartbeat_time = time.time()
        self._election_timeout = random.uniform(self.min_election_timeout, self.max_election_timeout)

    # ── Dynamic Peer Registration & Port-Range Discovery ──────────────────────

    def register_peer(self, node_id: str, raft_addr: str, app_addr: str = "") -> bool:
        """Dynamically add or update a peer in the live cluster."""
        if not node_id or not raft_addr:
            return False
        with self._lock:
            is_new = node_id not in self.peers
            self.peers[node_id] = raft_addr
            if app_addr:
                self.app_peers[node_id] = app_addr
            if node_id != self.node_id and node_id not in self._peer_stubs:
                self._peer_stubs[node_id] = raft_pb2_grpc.RaftServiceStub(grpc.insecure_channel(raft_addr, options=_GRPC_OPTS))
                self.next_index[node_id] = 1
                self.match_index[node_id] = 0
                logger.info("[RAFT] Dynamically added peer %s (raft=%s, app=%s)", node_id, raft_addr, app_addr)
                if self.state == NodeState.LEADER:
                    self._send_append_entries_all(is_heartbeat=True)
            return is_new

    def AddPeer(self, request, context):
        """gRPC handler allowing newly spawned nodes to join the cluster dynamically."""
        is_new = self.register_peer(request.node_id, request.raft_addr, request.app_addr)
        if is_new:
            for pid, stub in list(self._peer_stubs.items()):
                if pid != request.node_id:
                    self._rpc_executor.submit(
                        lambda s=stub: s.AddPeer(request, timeout=0.3) if hasattr(s, "AddPeer") else None
                    )
        with self._lock:
            peer_list = [
                raft_pb2.PeerInfo(node_id=pid, raft_addr=r_addr, app_addr=self.app_peers.get(pid, ""))
                for pid, r_addr in self.peers.items()
            ]
            return raft_pb2.PeerReply(success=True, leader_id=self.leader_id or "", term=self.current_term, peers=peer_list)

    def discover_port_range(self, host: str, start_port: int, end_port: int, app_port_offset: int = -20):
        """Probe active Raft ports in [start_port, end_port] and join dynamically."""
        my_info = raft_pb2.PeerInfo(node_id=self.node_id, raft_addr=self.self_raft_addr, app_addr=self.self_app_addr)
        for p in range(start_port, end_port + 1):
            addr = f"{host}:{p}"
            if addr == self.self_raft_addr:
                continue
            self._probe_and_join(addr, my_info)

    def discover_docker_dns(self, service_name: str, raft_port: int, app_port: int):
        """Continuously discover peer containers via Docker Embedded DNS (A-records for service_name)."""
        import socket
        my_info = raft_pb2.PeerInfo(node_id=self.node_id, raft_addr=self.self_raft_addr, app_addr=self.self_app_addr)
        while self._running:
            try:
                _, _, ip_list = socket.gethostbyname_ex(service_name)
                known_addrs = set(self.peers.values())
                for ip in ip_list:
                    addr = f"{ip}:{raft_port}"
                    if addr != self.self_raft_addr and addr not in known_addrs:
                        self._probe_and_join(addr, my_info, default_app_addr=f"{ip}:{app_port}")
            except Exception as exc:
                logger.debug("[RAFT-DNS] Discovery lookup for %s: %s", service_name, exc)
            time.sleep(1.5)

    def _probe_and_join(self, target_raft_addr: str, my_info, default_app_addr: str = ""):
        try:
            ch = grpc.insecure_channel(target_raft_addr, options=_GRPC_OPTS)
            reply = raft_pb2_grpc.RaftServiceStub(ch).AddPeer(my_info, timeout=0.3)
            if reply.success:
                for pi in reply.peers:
                    self.register_peer(pi.node_id, pi.raft_addr, pi.app_addr or default_app_addr)
                if reply.leader_id and (reply.term > self.current_term or (reply.term == self.current_term and self.state != NodeState.LEADER)):
                    with self._lock:
                        self._become_follower(reply.term, reply.leader_id)
            ch.close()
        except Exception:
            pass

    # ── State Transitions & Background Loops ──────────────────────────────────

    def _become_follower(self, term: int, leader_id: Optional[str] = None):
        self.state = NodeState.FOLLOWER
        if term > self.current_term:
            self.current_term, self.voted_for = term, None
            self.log.update_term_and_vote(term, None)
        self.leader_id = leader_id
        self._reset_election_timer()

    def _become_leader(self):
        self.state, self.leader_id = NodeState.LEADER, self.node_id
        last_idx = self.log.last_index()
        for pid in self.peers:
            if pid != self.node_id:
                self.next_index[pid] = last_idx + 1
                self.match_index[pid] = 0
        logger.info("[RAFT] Node %s -> LEADER for term %d", self.node_id, self.current_term)
        self._send_append_entries_all(is_heartbeat=True)

    def _run_ticker(self):
        while self._running:
            time.sleep(0.02)
            with self._lock:
                elapsed = time.time() - self._last_heartbeat_time
                if self.state == NodeState.LEADER and elapsed >= self.heartbeat_interval:
                    self._last_heartbeat_time = time.time()
                    self._send_append_entries_all(is_heartbeat=True)
                elif self.state != NodeState.LEADER and elapsed >= self._election_timeout:
                    self._start_election()

    def _run_applier(self):
        while self._running:
            with self._lock:
                while self.commit_index <= self.last_applied and self._running:
                    self._apply_cv.wait(timeout=0.1)
                if not self._running:
                    break
                entries = [self.log.get(i) for i in range(self.last_applied + 1, self.commit_index + 1)]
                self.last_applied = self.commit_index
            for entry in filter(None, entries):
                res = self.state_machine.apply(entry)
                with self._lock:
                    if entry.index in self._pending_events:
                        self._pending_results[entry.index] = res
                        self._pending_events[entry.index].set()

    # ── Elections & Replication ───────────────────────────────────────────────

    def _start_election(self):
        if os.environ.get("CLUSTER_DNS") and len(self.peers) < 2:
            self._reset_election_timer()
            return
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for, self.leader_id = self.node_id, None
        self.log.update_term_and_vote(self.current_term, self.node_id)
        self._reset_election_timer()
        self._votes = {self.node_id}
        if len(self._votes) >= (len(self.peers) // 2) + 1:
            self._become_leader()
            return
        term, last_idx, last_term = self.current_term, self.log.last_index(), self.log.last_term()
        for pid, stub in list(self._peer_stubs.items()):
            args = raft_pb2.RequestVoteArgs(**{"from": self.node_id, "to": pid, "term": term, "last_log_index": last_idx, "last_log_term": last_term})
            if self._running:
                self._rpc_executor.submit(self._vote_rpc, pid, stub, args)

    def _vote_rpc(self, pid: str, stub, args):
        try:
            rep = stub.RequestVote(args, timeout=0.15)
            with self._lock:
                if self.state != NodeState.CANDIDATE or self.current_term != args.term:
                    return
                if rep.term > self.current_term:
                    self._become_follower(rep.term)
                elif rep.vote_granted and rep.term == self.current_term:
                    self._votes.add(pid)
                    if len(self._votes) >= (len(self.peers) // 2) + 1:
                        self._become_leader()
        except Exception:
            pass

    def _send_append_entries_all(self, is_heartbeat: bool = False):
        if self.state != NodeState.LEADER or not self._running:
            return
        for pid, stub in list(self._peer_stubs.items()):
            prev_idx = self.next_index.get(pid, self.log.last_index() + 1) - 1
            entries = [
                raft_pb2.LogEntry(index=e.index, term=e.term, command_type=e.command_type, payload=json.dumps(e.payload), request_id=e.request_id)
                for e in self.log.entries_from(prev_idx + 1)
            ]
            args = raft_pb2.AppendEntriesArgs(
                **{"from": self.node_id, "to": pid, "term": self.current_term, "prev_index": prev_idx, "prev_term": self.log.term_at(prev_idx), "commit_index": self.commit_index, "entries": entries}
            )
            try:
                self._rpc_executor.submit(self._append_rpc, pid, stub, args)
            except RuntimeError:
                pass

    def _append_rpc(self, pid: str, stub, args):
        try:
            rep = stub.AppendEntries(args, timeout=0.15)
            with self._lock:
                if self.state != NodeState.LEADER or self.current_term != args.term:
                    return
                if rep.term > self.current_term:
                    self._become_follower(rep.term)
                elif rep.entry_appended:
                    self.match_index[pid] = rep.match_index
                    self.next_index[pid] = rep.match_index + 1
                    self._advance_commit_index()
                else:
                    self.next_index[pid] = max(1, self.next_index.get(pid, 1) - 1)
        except Exception:
            pass

    def _advance_commit_index(self):
        needed = (len(self.peers) // 2) + 1
        for idx in range(self.log.last_index(), self.commit_index, -1):
            if self.log.term_at(idx) == self.current_term:
                replicas = 1 + sum(1 for p in self.peers if p != self.node_id and self.match_index.get(p, 0) >= idx)
                if replicas >= needed:
                    self.commit_index = idx
                    self._apply_cv.notify_all()
                    self._send_append_entries_all(is_heartbeat=True)
                    break

    # ── Assignment Page 7 gRPC Handlers ───────────────────────────────────────

    def RequestVote(self, request, context):
        with self._lock:
            sender = getattr(request, "from")
            if request.term < self.current_term:
                return raft_pb2.RequestVoteReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "vote_granted": False})
            if request.term > self.current_term:
                self._become_follower(request.term)
            can_vote = self.voted_for in (None, "", sender)
            lt, li = self.log.last_term(), self.log.last_index()
            log_ok = (request.last_log_term > lt) or (request.last_log_term == lt and request.last_log_index >= li)
            if can_vote and log_ok:
                self.voted_for = sender
                self.log.set_voted_for(sender)
                self._reset_election_timer()
            return raft_pb2.RequestVoteReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "vote_granted": bool(can_vote and log_ok)})

    def AppendEntries(self, request, context):
        with self._lock:
            sender = getattr(request, "from")
            if request.term < self.current_term:
                return raft_pb2.AppendEntriesReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "entry_appended": False, "match_index": self.log.last_index()})
            if request.term > self.current_term or self.state != NodeState.FOLLOWER:
                self._become_follower(request.term, sender)
            else:
                self.leader_id = sender
                self._reset_election_timer()

            if request.prev_index > 0:
                if self.log.last_index() < request.prev_index:
                    return raft_pb2.AppendEntriesReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "entry_appended": False, "match_index": self.log.last_index()})
                if self.log.term_at(request.prev_index) != request.prev_term:
                    self.log.truncate_from(request.prev_index)
                    return raft_pb2.AppendEntriesReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "entry_appended": False, "match_index": self.log.last_index()})

            for e in request.entries:
                ex = self.log.get(e.index)
                if ex and ex.term != e.term:
                    self.log.truncate_from(e.index)
                self.log.append_entry(LogEntry(e.index, e.term, e.command_type, e.payload, e.request_id))

            if request.commit_index > self.commit_index:
                self.commit_index = min(request.commit_index, self.log.last_index())
                self._apply_cv.notify_all()
            return raft_pb2.AppendEntriesReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "entry_appended": True, "match_index": self.log.last_index()})

    def propose(self, command_type: str, payload: dict, request_id: str = "", timeout: float = 4.0) -> Tuple[bool, str, Optional[str], dict]:
        with self._lock:
            if self.state != NodeState.LEADER:
                return False, f"Not leader (leader={self.leader_id})", self.leader_id, {}
            entry = self.log.append(self.current_term, command_type, payload, request_id)
            ev = threading.Event()
            self._pending_events[entry.index] = ev
            if len(self.peers) <= 1:
                self.commit_index = entry.index
                self._apply_cv.notify_all()
        self._send_append_entries_all()
        ok = ev.wait(timeout=timeout)
        with self._lock:
            self._pending_events.pop(entry.index, None)
            res = self._pending_results.pop(entry.index, {})
        return (True, "", self.node_id, res) if ok else (False, f"Raft proposal timed out after {timeout}s", self.node_id, {})

    def status(self) -> dict:
        with self._lock:
            return {"node_id": self.node_id, "state": self.state.value, "term": self.current_term, "leader_id": self.leader_id or "", "commit_index": self.commit_index, "last_applied": self.last_applied, "peers": list(self.peers.keys())}

    def stop(self):
        self._running = False
        with self._apply_cv:
            self._apply_cv.notify_all()
        for t in (self._ticker_thread, self._applier_thread):
            if t.is_alive() and t is not threading.current_thread():
                t.join(timeout=0.2)
        self._rpc_executor.shutdown(wait=False)
