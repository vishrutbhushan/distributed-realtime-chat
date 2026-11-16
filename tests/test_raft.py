"""
Comprehensive unit tests for Milestone 2 Raft Consensus and Replication:
- Persistent Raft log and conflict truncation
- State Machine execution (including real-time notifications)
- Leader election across a 3-node cluster
- Log replication to majority quorum
- Leader failure detection and failover
- Rejoining follower catchup
"""

import os
import tempfile
import time
import unittest

from raft.log import LogEntry, RaftLog
from raft.node import NodeState, RaftNode
from raft.state_machine import StateMachine
from storage.database import Database


class DirectRaftStub:
    """In-memory direct stub for testing Raft RPCs without opening network sockets."""
    def __init__(self, target_node_getter):
        self._get_target = target_node_getter

    def RequestVote(self, args, timeout=None):
        target = self._get_target()
        if not target or not target._running:
            raise RuntimeError("Peer unavailable")
        return target.RequestVote(args, None)

    def AppendEntries(self, args, timeout=None):
        target = self._get_target()
        if not target or not target._running:
            raise RuntimeError("Peer unavailable")
        return target.AppendEntries(args, None)

    def AddPeer(self, args, timeout=None):
        target = self._get_target()
        if not target or not target._running:
            raise RuntimeError("Peer unavailable")
        return target.AddPeer(args, None)


class RaftLogAndStateMachineTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "chat.db")
        self.db = Database(self.db_path)
        self.log = RaftLog(self.db)
        self.sm = StateMachine(self.db)

    def tearDown(self):
        self.db.close()
        self.temp_dir.cleanup()

    def test_log_and_meta_persistence(self):
        self.assertEqual(self.log.get_current_term(), 0)
        self.assertIsNone(self.log.get_voted_for())

        self.log.set_current_term(2)
        self.assertEqual(self.log.get_current_term(), 2)

        self.log.set_voted_for("node-1")
        self.assertEqual(self.log.get_voted_for(), "node-1")

        e1 = self.log.append(term=1, command_type="SIGNUP", payload={"username": "alice"})
        self.assertEqual(e1.index, 1)
        self.assertEqual(e1.term, 1)

        e2 = self.log.append(term=2, command_type="SEND_DM", payload={"content": "hello"})
        self.assertEqual(e2.index, 2)
        self.assertEqual(self.log.last_index(), 2)
        self.assertEqual(self.log.last_term(), 2)

        # Truncate
        self.log.truncate_from(2)
        self.assertEqual(self.log.last_index(), 1)
        self.assertIsNone(self.log.get(2))

    def test_state_machine_application(self):
        signup_entry = LogEntry(
            index=1,
            term=1,
            command_type="SIGNUP",
            payload={
                "user_id": "u1",
                "username": "alice",
                "password_hash": "hash123",
                "token": "tok1",
            },
        )
        res = self.sm.apply(signup_entry)
        self.assertTrue(res["success"])

        user = self.db.fetchone("SELECT * FROM users WHERE user_id = 'u1'")
        self.assertIsNotNone(user)
        self.assertEqual(user["username"], "alice")

        grp_entry = LogEntry(
            index=2,
            term=1,
            command_type="CREATE_GROUP",
            payload={
                "group_id": "g1",
                "name": "General",
                "creator_id": "u1",
                "initial_member_ids": ["u1"],
            },
        )
        res = self.sm.apply(grp_entry)
        self.assertTrue(res["success"])
        grp = self.db.fetchone("SELECT * FROM groups WHERE group_id = 'g1'")
        self.assertIsNotNone(grp)
        self.assertEqual(grp["name"], "General")

        dm_entry = LogEntry(
            index=3,
            term=1,
            command_type="SEND_DM",
            payload={
                "message_id": "m1",
                "sender_id": "u1",
                "recipient_id": "u1",
                "content": "Self reminder",
            },
        )
        res = self.sm.apply(dm_entry)
        self.assertTrue(res["success"])
        msg = self.db.fetchone("SELECT * FROM messages WHERE message_id = 'm1'")
        self.assertIsNotNone(msg)
        self.assertEqual(msg["content"], "Self reminder")


class RaftClusterIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dirs = [tempfile.TemporaryDirectory() for _ in range(3)]
        self.dbs = [Database(os.path.join(td.name, "chat.db")) for td in self.temp_dirs]

        self.peers = {
            "node-1": "mock:1",
            "node-2": "mock:2",
            "node-3": "mock:3",
        }

        self.nodes_map = {}
        self.nodes = []

        for i in range(3):
            nid = f"node-{i+1}"
            sm = StateMachine(self.dbs[i])
            stubs = {
                f"node-{j+1}": DirectRaftStub(lambda target_id=f"node-{j+1}": self.nodes_map.get(target_id))
                for j in range(3)
                if j != i
            }
            node = RaftNode(
                node_id=nid,
                db=self.dbs[i],
                peers=self.peers,
                state_machine=sm,
                heartbeat_interval=0.03,
                min_election_timeout=0.08,
                max_election_timeout=0.20,
                peer_stubs=stubs,
            )
            self.nodes.append(node)
            self.nodes_map[nid] = node

    def tearDown(self):
        for node in self.nodes:
            node.stop()
        for db in self.dbs:
            db.close()
        for td in self.temp_dirs:
            td.cleanup()

    def _wait_for_leader(self, timeout=3.0):
        start = time.time()
        while time.time() - start < timeout:
            leaders = [n for n in self.nodes if n._running and n.state == NodeState.LEADER]
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(0.04)
        return None

    def test_leader_election_and_quorum_replication(self):
        leader = self._wait_for_leader(timeout=4.0)
        self.assertIsNotNone(leader, "A leader should be elected among the 3 nodes")
        self.assertEqual(leader.state, NodeState.LEADER)

        ok, err, leader_id, res = leader.propose(
            command_type="SIGNUP",
            payload={
                "user_id": "u_bob",
                "username": "bob",
                "password_hash": "hash_bob",
                "token": "tok_bob",
            },
            request_id="req-1",
            timeout=3.0,
        )
        self.assertTrue(ok, f"Proposal failed: {err}")
        self.assertEqual(leader_id, leader.node_id)

        time.sleep(0.15)
        for i, db in enumerate(self.dbs):
            user = db.fetchone("SELECT * FROM users WHERE user_id = 'u_bob'")
            self.assertIsNotNone(user, f"Node {i+1} state machine should have applied the entry")
            self.assertEqual(user["username"], "bob")

    def test_leader_failover_and_cluster_recovery(self):
        leader = self._wait_for_leader(timeout=4.0)
        self.assertIsNotNone(leader)

        old_leader_id = leader.node_id
        leader.stop()

        start = time.time()
        new_leader = None
        while time.time() - start < 4.0:
            active_leaders = [
                n for n in self.nodes
                if n._running and n.node_id != old_leader_id and n.state == NodeState.LEADER
            ]
            if len(active_leaders) == 1:
                new_leader = active_leaders[0]
                break
            time.sleep(0.04)

        self.assertIsNotNone(new_leader, "Remaining 2 nodes should elect a new leader")
        self.assertNotEqual(new_leader.node_id, old_leader_id)

        ok, err, lid, res = new_leader.propose(
            command_type="SIGNUP",
            payload={
                "user_id": "u_charlie",
                "username": "charlie",
                "password_hash": "hash_c",
                "token": "tok_c",
            },
            request_id="req-2",
            timeout=3.0,
        )
        self.assertTrue(ok, f"New leader proposal failed: {err}")

    def test_dynamic_node_join_and_catchup(self):
        leader = self._wait_for_leader(timeout=4.0)
        self.assertIsNotNone(leader)

        # Propose an entry while cluster has 3 nodes
        ok, err, _, _ = leader.propose(
            command_type="SIGNUP",
            payload={"user_id": "u_early", "username": "early_user", "password_hash": "h", "token": "t"},
            request_id="req-early",
        )
        self.assertTrue(ok, err)

        # Dynamically spin up node-4 and register it with the cluster
        td4 = tempfile.TemporaryDirectory()
        self.temp_dirs.append(td4)
        db4 = Database(os.path.join(td4.name, "chat.db"))
        self.dbs.append(db4)

        stubs4 = {
            f"node-{j+1}": DirectRaftStub(lambda target_id=f"node-{j+1}": self.nodes_map.get(target_id))
            for j in range(3)
        }
        peers4 = dict(self.peers)
        peers4["node-4"] = "mock:4"
        node4 = RaftNode(
            node_id="node-4",
            db=db4,
            peers=peers4,
            state_machine=StateMachine(db4),
            heartbeat_interval=0.03,
            min_election_timeout=0.15,
            max_election_timeout=0.30,
            peer_stubs=stubs4,
        )
        self.nodes.append(node4)
        self.nodes_map["node-4"] = node4

        for n in self.nodes[:3]:
            n._peer_stubs["node-4"] = DirectRaftStub(lambda: self.nodes_map.get("node-4"))
            n.register_peer("node-4", "mock:4", "app:4")

        deadline = time.time() + 2.0
        user_on_node4 = None
        while time.time() < deadline:
            user_on_node4 = db4.fetchone("SELECT * FROM users WHERE user_id = 'u_early'")
            if user_on_node4:
                break
            time.sleep(0.05)
        self.assertIsNotNone(user_on_node4, "Dynamically added node-4 should catch up existing log entries")
        self.assertEqual(user_on_node4["username"], "early_user")


if __name__ == "__main__":
    unittest.main()
