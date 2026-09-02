"""
Raft node (Milestone 2 stub).

In M1 the node is in STANDALONE state — no election, no replication.
In M2 this will implement the full Raft protocol:
  - RequestVote / AppendEntries RPCs
  - Election timeout + randomisation
  - Leader heartbeat
  - Log replication to followers
  - Majority commit
"""

import logging
import os
import time
from enum import Enum

from raft.log import RaftLog
from raft.state_machine import StateMachine

logger = logging.getLogger(__name__)


class NodeState(str, Enum):
    STANDALONE = "STANDALONE"   # M1 only
    FOLLOWER   = "FOLLOWER"
    CANDIDATE  = "CANDIDATE"
    LEADER     = "LEADER"


class RaftNode:
    """
    Raft consensus node.

    Milestone 1: STANDALONE — wraps RaftLog so that every committed operation
    is recorded, making M2 upgrade straightforward.

    Milestone 2: implement election and replication here.
    """

    def __init__(self, node_id: str, db, peers: list = None):
        self.node_id   = node_id
        self.db        = db
        self.peers     = peers or []   # list of peer addresses (host:port)

        self.state     = NodeState.STANDALONE
        self.term      = 0
        self.voted_for = None
        self.leader_id = node_id       # in STANDALONE, self is leader

        self.commit_index = 0
        self.last_applied = 0

        self.log       = RaftLog(db)

        logger.info(
            "[RAFT] Node %s started in %s mode  peers=%s",
            node_id, self.state, self.peers,
        )

    # ── M1 helpers ────────────────────────────────────────────────────────

    def is_leader(self) -> bool:
        """M1: always True (standalone). M2: check state == LEADER."""
        return True

    def status(self) -> dict:
        return {
            "node_id":      self.node_id,
            "state":        self.state.value,
            "term":         self.term,
            "leader_id":    self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
        }

    # ── Milestone 2 stubs ─────────────────────────────────────────────────
    # TODO M2: Implement the methods below.

    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        """Handle incoming RequestVote RPC."""
        raise NotImplementedError("Implement in Milestone 2")

    def append_entries(
        self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit
    ):
        """Handle incoming AppendEntries RPC."""
        raise NotImplementedError("Implement in Milestone 2")

    def _start_election(self):
        """Transition to CANDIDATE and start an election."""
        raise NotImplementedError("Implement in Milestone 2")

    def _send_heartbeats(self):
        """Send empty AppendEntries to all peers (leader only)."""
        raise NotImplementedError("Implement in Milestone 2")

    def _replicate_log(self, entries):
        """Replicate new entries to followers and wait for majority ack."""
        raise NotImplementedError("Implement in Milestone 2")
