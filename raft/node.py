"""Compact Raft Consensus Engine with Dynamic Port-Range Peer Discovery."""

from concurrent import futures
from enum import Enum
import json
import logging
import random
import threading
import time
from typing import Any, Dict, Optional, Tuple

import grpc
import raft_pb2
import raft_pb2_grpc
from raft.log import LogEntry, RaftLog
from raft.state_machine import StateMachine
from raft.telemetry import RaftTelemetry

logger = logging.getLogger(__name__)

_GRPC_OPTS = [
    ("grpc.max_send_message_length", 64 * 1024 * 1024),
    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 500),
    ("grpc.dns_min_time_between_resolutions_ms", 1000),
]


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
        voter_ids: Optional[frozenset[str]] = None,
        startup_grace: float = 0.0,
    ):
        self.node_id = str(node_id)
        self.db = db
        self.peers = dict(peers)
        self.voter_ids = voter_ids if voter_ids is not None else frozenset(peers)
        self.quorum = len(self.voter_ids) // 2 + 1
        self._voter_addresses = {pid: self.peers[pid] for pid in self.voter_ids if pid in self.peers}
        self._voter_app_addresses = {pid: app_peers[pid] for pid in self.voter_ids if app_peers and pid in app_peers}
        self.app_peers = dict(app_peers or {})
        self.self_raft_addr = self_raft_addr or self.peers.get(self.node_id, "")
        self.self_app_addr = self_app_addr or self.app_peers.get(self.node_id, "")
        self.log = RaftLog(db)
        self.state_machine = state_machine or StateMachine(db)
        self.telemetry = RaftTelemetry()

        self._lock = threading.RLock()
        self.current_term = self.log.get_current_term()
        self.voted_for = self.log.get_voted_for()
        self.commit_index = 0
        self.last_applied = self.state_machine.last_applied
        self.state = NodeState.FOLLOWER
        self._pre_vote_round = 0
        self.leader_id: Optional[str] = None
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self.heartbeat_interval = heartbeat_interval
        self.min_election_timeout = min_election_timeout
        self.max_election_timeout = max_election_timeout
        self._reset_election_timer()
        self._election_timeout += startup_grace

        self._pending_events: Dict[tuple[int, int], threading.Event] = {}
        self._pending_results: Dict[tuple[int, int], Any] = {}
        self._append_in_flight: set[str] = set()
        self._peer_channels: Dict[str, grpc.Channel] = {}
        self._peer_stubs: Dict[str, Any] = dict(peer_stubs or {})
        if not self._peer_stubs:
            for pid, addr in self.peers.items():
                if pid != self.node_id:
                    self._connect_peer(pid, addr)

        self._rpc_executor = futures.ThreadPoolExecutor(max_workers=16, thread_name_prefix="raft-rpc")
        self._running = True
        self._apply_cv = threading.Condition(self._lock)
        self._ticker_thread = threading.Thread(target=self._run_ticker, daemon=True)
        self._applier_thread = threading.Thread(target=self._run_applier, daemon=True)
        self._ticker_thread.start()
        self._applier_thread.start()

        if self.voter_ids == {self.node_id}:
            self._become_leader()

    def _reset_election_timer(self):
        self._last_heartbeat_time = time.time()
        self._election_timeout = random.uniform(self.min_election_timeout, self.max_election_timeout)

    # ── Dynamic Peer Registration & Port-Range Discovery ──────────────────────

    def _connect_peer(self, node_id: str, addr: str):
        old_channel = self._peer_channels.pop(node_id, None)
        if old_channel is not None:
            old_channel.close()
        channel = grpc.insecure_channel(addr, options=_GRPC_OPTS)
        self._peer_channels[node_id] = channel
        self._peer_stubs[node_id] = raft_pb2_grpc.RaftServiceStub(channel)

    def register_peer(self, node_id: str, raft_addr: str, app_addr: str = "") -> bool:
        """Dynamically add or update a peer in the live cluster."""
        if not node_id or node_id == self.node_id or not raft_addr:
            return False
        with self._lock:
            if not self._running:
                return False
            raft_addr = self._voter_addresses.get(node_id, raft_addr)
            app_addr = self._voter_app_addresses.get(node_id, app_addr)
            old_addr = self.peers.get(node_id)
            self.peers[node_id] = raft_addr
            if app_addr:
                self.app_peers[node_id] = app_addr
            if node_id not in self._peer_stubs or (old_addr is not None and old_addr != raft_addr):
                self._connect_peer(node_id, raft_addr)
            if old_addr != raft_addr:
                self.next_index[node_id] = 1
                self.match_index[node_id] = 0
                logger.info("[RAFT] Dynamically added peer %s (raft=%s, app=%s)", node_id, raft_addr, app_addr)
                if self.state == NodeState.LEADER:
                    self._send_append_entries_all(is_heartbeat=True)
            return old_addr is None

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

    def discover_docker_dns(self, service_name: str, raft_port: int):
        """Continuously discover peer containers via Docker Embedded DNS (A-records for service_name)."""
        import socket
        my_info = raft_pb2.PeerInfo(node_id=self.node_id, raft_addr=self.self_raft_addr, app_addr=self.self_app_addr)
        seen = set()
        missing = {}
        voter_ips = {}
        while self._running:
            try:
                _, _, ip_list = socket.gethostbyname_ex(service_name)
                own_ip = socket.gethostbyname(socket.gethostname())
                live = {f"{ip}:{raft_port}" for ip in ip_list if ip != own_ip}
                for pid, addr in self._voter_addresses.items():
                    if pid == self.node_id:
                        continue
                    try:
                        ip = socket.gethostbyname(addr.rsplit(":", 1)[0])
                    except OSError:
                        voter_ips.pop(pid, None)
                        continue
                    if voter_ips.get(pid) != ip:
                        with self._lock:
                            if self._running:
                                self._connect_peer(pid, addr)
                        voter_ips[pid] = ip
                seen.intersection_update(live)
                for addr in live - seen:
                    if self._probe_and_join(addr, my_info):
                        seen.add(addr)
                with self._lock:
                    for pid, addr in list(self.peers.items()):
                        if pid == self.node_id or pid in self.voter_ids:
                            continue
                        if addr in live:
                            missing.pop(pid, None)
                            continue
                        missing[pid] = missing.get(pid, 0) + 1
                        if missing[pid] >= 3:
                            self.peers.pop(pid, None)
                            self.app_peers.pop(pid, None)
                            self.next_index.pop(pid, None)
                            self.match_index.pop(pid, None)
                            self._peer_stubs.pop(pid, None)
                            channel = self._peer_channels.pop(pid, None)
                            if channel is not None:
                                channel.close()
                            missing.pop(pid, None)
            except Exception as exc:
                logger.debug("[RAFT-DNS] Discovery lookup for %s: %s", service_name, exc)
            time.sleep(1.5)

    def _probe_and_join(self, target_raft_addr: str, my_info, default_app_addr: str = ""):
        try:
            with grpc.insecure_channel(target_raft_addr, options=_GRPC_OPTS) as channel:
                reply = raft_pb2_grpc.RaftServiceStub(channel).AddPeer(my_info, timeout=0.3)
            if reply.success:
                for pi in reply.peers:
                    self.register_peer(pi.node_id, pi.raft_addr, pi.app_addr or default_app_addr)
                return True
        except Exception:
            pass
        return False

    # ── State Transitions & Background Loops ──────────────────────────────────

    def _become_follower(self, term: int, leader_id: Optional[str] = None):
        changed = self.state != NodeState.FOLLOWER or term > self.current_term or leader_id != self.leader_id
        self.state = NodeState.FOLLOWER
        self._pre_vote_round += 1
        if term > self.current_term:
            self.current_term, self.voted_for = term, None
            self.log.update_term_and_vote(term, None)
        self.leader_id = leader_id
        self._reset_election_timer()
        if changed:
            self.telemetry.emit("follower", self.current_term, leader=leader_id)

    def _become_leader(self):
        self.state, self.leader_id = NodeState.LEADER, self.node_id
        last_idx = self.log.last_index()
        for pid in self.peers:
            if pid != self.node_id:
                self.next_index[pid] = last_idx + 1
                self.match_index[pid] = 0
        logger.info("[RAFT] Node %s -> LEADER for term %d", self.node_id, self.current_term)
        self.telemetry.emit("leader_elected", self.current_term, voters=sorted(getattr(self, "_votes", {self.node_id})))
        entry = self.log.append(self.current_term, "NOOP", {})
        self._trace_entry("log_appended", entry)
        if self.quorum == 1:
            with self._apply_cv:
                self.commit_index = self.log.last_index()
                self._apply_cv.notify_all()
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
                    self._start_pre_vote()

    def _run_applier(self):
        while self._running:
            with self._lock:
                while self.commit_index <= self.last_applied and self._running:
                    self._apply_cv.wait(timeout=0.1)
                if not self._running:
                    break
                entries = [self.log.get(i) for i in range(self.last_applied + 1, self.commit_index + 1)]
            for entry in filter(None, entries):
                res = self.state_machine.apply(entry)
                with self._lock:
                    self.last_applied = entry.index
                    self._apply_cv.notify_all()
                    self._trace_entry("entry_applied", entry, success=bool(res.get("success")))
                    key = (entry.term, entry.index)
                    if key in self._pending_events:
                        self._pending_results[key] = {**res, "applied_index": entry.index}
                        self._pending_events[key].set()

    # ── Elections & Replication ───────────────────────────────────────────────

    def _vote_request(self, pid: str, term: int):
        return raft_pb2.RequestVoteArgs(**{
            "from": self.node_id, "to": pid, "term": term,
            "last_log_index": self.log.last_index(), "last_log_term": self.log.last_term(),
        })

    def _start_pre_vote(self):
        self.leader_id = None
        self._reset_election_timer()
        if self.node_id not in self.voter_ids:
            return
        if self.quorum == 1:
            self._start_election()
            return
        self._pre_vote_round += 1
        round_id = self._pre_vote_round
        self._pre_votes = {self.node_id}
        self.telemetry.emit("pre_vote_started", self.current_term + 1)
        for pid, stub in self._peer_stubs.items():
            if pid in self.voter_ids and self._running:
                args = self._vote_request(pid, self.current_term + 1)
                self._rpc_executor.submit(self._pre_vote_rpc, pid, stub, args, round_id)

    def _pre_vote_rpc(self, pid: str, stub, args, round_id: int):
        try:
            reply = stub.PreVote(args, timeout=0.15)
            with self._lock:
                if not self._running or self.state == NodeState.LEADER or self._pre_vote_round != round_id:
                    return
                self.telemetry.emit("pre_vote_reply", args.term, peer=pid, granted=reply.vote_granted)
                if reply.term > self.current_term:
                    self._become_follower(reply.term)
                elif reply.vote_granted:
                    self._pre_votes.add(pid)
                    if len(self._pre_votes) >= self.quorum:
                        self._start_election()
        except Exception:
            pass

    def _start_election(self):
        self._pre_vote_round += 1
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for, self.leader_id = self.node_id, None
        self.log.update_term_and_vote(self.current_term, self.node_id)
        self._reset_election_timer()
        self._votes = {self.node_id}
        self.telemetry.emit("election_started", self.current_term, quorum=self.quorum)
        if len(self._votes) >= self.quorum:
            self._become_leader()
            return
        for pid, stub in list(self._peer_stubs.items()):
            if pid not in self.voter_ids:
                continue
            args = self._vote_request(pid, self.current_term)
            if self._running:
                self._rpc_executor.submit(self._vote_rpc, pid, stub, args)

    def _vote_rpc(self, pid: str, stub, args):
        try:
            rep = stub.RequestVote(args, timeout=0.15)
            with self._lock:
                if self.state != NodeState.CANDIDATE or self.current_term != args.term:
                    return
                self.telemetry.emit("vote_reply", args.term, peer=pid, granted=rep.vote_granted)
                if rep.term > self.current_term:
                    self._become_follower(rep.term)
                elif rep.vote_granted and rep.term == self.current_term:
                    self._votes.add(pid)
                    if len(self._votes) >= self.quorum:
                        self._become_leader()
        except Exception:
            pass

    def _send_append_entries_all(self, is_heartbeat: bool = False):
        if self.state != NodeState.LEADER or not self._running:
            return
        for pid, stub in list(self._peer_stubs.items()):
            if pid in self._append_in_flight:
                continue
            prev_idx = self.next_index.get(pid, self.log.last_index() + 1) - 1
            entries = [
                raft_pb2.LogEntry(index=e.index, term=e.term, command_type=e.command_type, payload=json.dumps(e.payload), request_id=e.request_id)
                for e in self.log.entries_from(prev_idx + 1)
            ]
            args = raft_pb2.AppendEntriesArgs(
                **{"from": self.node_id, "to": pid, "term": self.current_term, "prev_index": prev_idx, "prev_term": self.log.term_at(prev_idx), "commit_index": self.commit_index, "entries": entries}
            )
            if entries:
                self.telemetry.emit("append_sent", self.current_term, peer=pid, first=entries[0].index, last=entries[-1].index)
            try:
                self._append_in_flight.add(pid)
                self._rpc_executor.submit(self._append_rpc, pid, stub, args)
            except RuntimeError:
                self._append_in_flight.discard(pid)

    def _append_rpc(self, pid: str, stub, args):
        try:
            rep = stub.AppendEntries(args, timeout=0.15)
            with self._lock:
                if self.state != NodeState.LEADER or self.current_term != args.term or self._peer_stubs.get(pid) is not stub:
                    return
                if rep.term > self.current_term:
                    self._become_follower(rep.term)
                elif rep.entry_appended:
                    matched = args.entries[-1].index if args.entries else args.prev_index
                    if matched > self.match_index.get(pid, 0):
                        self.telemetry.emit("append_ack", args.term, peer=pid, index=matched)
                    self.match_index[pid] = matched
                    self.next_index[pid] = matched + 1
                    self._advance_commit_index()
                else:
                    self.next_index[pid] = max(1, self.next_index.get(pid, 1) - 1)
        except Exception:
            pass
        finally:
            with self._lock:
                self._append_in_flight.discard(pid)

    def _advance_commit_index(self):
        for idx in range(self.log.last_index(), self.commit_index, -1):
            if self.log.term_at(idx) == self.current_term:
                replicas = 1 + sum(1 for p in self.voter_ids if p != self.node_id and self.match_index.get(p, 0) >= idx)
                if replicas >= self.quorum:
                    previous = self.commit_index
                    self.commit_index = idx
                    voters = sorted(p for p in self.voter_ids if p == self.node_id or self.match_index.get(p, 0) >= idx)
                    self.telemetry.emit("commit_advanced", self.current_term, first=previous + 1, last=idx, voters=voters)
                    self._apply_cv.notify_all()
                    self._send_append_entries_all(is_heartbeat=True)
                    break

    # ── Assignment Page 7 gRPC Handlers ───────────────────────────────────────

    def _candidate_log_is_up_to_date(self, request):
        return (request.last_log_term, request.last_log_index) >= (self.log.last_term(), self.log.last_index())

    def PreVote(self, request, context):
        with self._lock:
            sender = getattr(request, "from")
            leader_recently_seen = self.leader_id and time.time() - self._last_heartbeat_time < self._election_timeout
            granted = (
                self.node_id in self.voter_ids and sender in self.voter_ids
                and request.term > self.current_term and self.state != NodeState.LEADER
                and not leader_recently_seen and self._candidate_log_is_up_to_date(request)
            )
            return raft_pb2.RequestVoteReply(**{
                "from": self.node_id, "to": sender, "term": self.current_term, "vote_granted": granted,
            })

    def RequestVote(self, request, context):
        with self._lock:
            sender = getattr(request, "from")
            if self.node_id not in self.voter_ids or sender not in self.voter_ids or request.term < self.current_term:
                return raft_pb2.RequestVoteReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "vote_granted": False})
            if request.term > self.current_term:
                self._become_follower(request.term)
            can_vote = self.voted_for in (None, "", sender)
            log_ok = self._candidate_log_is_up_to_date(request)
            if can_vote and log_ok:
                self._pre_vote_round += 1
                self.voted_for = sender
                self.log.set_voted_for(sender)
                self._reset_election_timer()
            self.telemetry.emit("vote_cast", request.term, peer=sender, granted=bool(can_vote and log_ok))
            return raft_pb2.RequestVoteReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "vote_granted": bool(can_vote and log_ok)})

    def AppendEntries(self, request, context):
        with self._lock:
            sender = getattr(request, "from")
            if sender not in self.voter_ids or request.term < self.current_term:
                return raft_pb2.AppendEntriesReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "entry_appended": False, "match_index": self.log.last_index()})
            if request.term > self.current_term or self.state != NodeState.FOLLOWER:
                self._become_follower(request.term, sender)
            else:
                if self.leader_id != sender:
                    self.telemetry.emit("follower", self.current_term, leader=sender)
                self.leader_id = sender
                self._pre_vote_round += 1
                self._reset_election_timer()

            if request.prev_index > 0:
                if self.log.last_index() < request.prev_index:
                    return raft_pb2.AppendEntriesReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "entry_appended": False, "match_index": self.log.last_index()})
                if self.log.term_at(request.prev_index) != request.prev_term:
                    return raft_pb2.AppendEntriesReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "entry_appended": False, "match_index": self.log.last_index()})

            for e in request.entries:
                ex = self.log.get(e.index)
                if ex and ex.term == e.term:
                    continue
                if ex and ex.term != e.term:
                    self.log.truncate_from(e.index)
                entry = LogEntry(e.index, e.term, e.command_type, e.payload, e.request_id)
                self.log.append_entry(entry)
                self._trace_entry("log_appended", entry, leader=sender)

            matched = request.entries[-1].index if request.entries else request.prev_index
            if min(request.commit_index, matched) > self.commit_index:
                previous = self.commit_index
                self.commit_index = min(request.commit_index, matched)
                self.telemetry.emit("commit_advanced", self.current_term, first=previous + 1, last=self.commit_index, leader=sender)
                self._apply_cv.notify_all()
            return raft_pb2.AppendEntriesReply(**{"from": self.node_id, "to": sender, "term": self.current_term, "entry_appended": True, "match_index": matched})

    def propose(self, command_type: str, payload: dict, request_id: str = "", timeout: float = 4.0) -> Tuple[bool, str, Optional[str], dict]:
        with self._lock:
            if self.state != NodeState.LEADER:
                return False, f"Not leader (leader={self.leader_id})", self.leader_id, {}
            entry = self.log.append(self.current_term, command_type, payload, request_id)
            self._trace_entry("log_appended", entry)
            ev = threading.Event()
            key = (entry.term, entry.index)
            self._pending_events[key] = ev
            if self.quorum == 1:
                self.commit_index = entry.index
                self._apply_cv.notify_all()
            self._send_append_entries_all()
        ok = ev.wait(timeout=timeout)
        with self._lock:
            self._pending_events.pop(key, None)
            res = self._pending_results.pop(key, {})
        return (True, "", self.node_id, res) if ok else (False, f"Raft proposal timed out after {timeout}s", self.node_id, {})

    def status(self) -> dict:
        with self._lock:
            return {"node_id": self.node_id, "state": self.state.value, "term": self.current_term, "leader_id": self.leader_id or "", "commit_index": self.commit_index, "last_applied": self.last_applied, "peers": list(self.peers.keys())}

    def wait_applied(self, index: int, timeout: float) -> bool:
        with self._apply_cv:
            self._apply_cv.wait_for(lambda: self.last_applied >= index or not self._running, timeout)
            return self.last_applied >= index

    def _trace_entry(self, kind, entry, **fields):
        preview = ""
        if isinstance(entry.payload, dict):
            preview = str(entry.payload.get("content", ""))[:32]
        self.telemetry.emit(
            kind, entry.term, index=entry.index, command=entry.command_type,
            request_id=entry.request_id, preview=preview, **fields,
        )

    def monitor(self, after, boot):
        active_users = self.active_users_fn() if getattr(self, "active_users_fn", None) else []
        with self._lock:
            return {
                **self.status(),
                "active_users": active_users,
                "voters": sorted(self.voter_ids),
                "quorum": self.quorum,
                "last_index": self.log.last_index(),
                "match_index": dict(self.match_index) if self.state == NodeState.LEADER else {},
                **self.telemetry.read(after, boot),
            }

    def stop(self):
        self._running = False
        with self._apply_cv:
            self._apply_cv.notify_all()
        for t in (self._ticker_thread, self._applier_thread):
            if t.is_alive() and t is not threading.current_thread():
                t.join(timeout=0.2)
        self._rpc_executor.shutdown(wait=True, cancel_futures=True)
        for channel in self._peer_channels.values():
            channel.close()
