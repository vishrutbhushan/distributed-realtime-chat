# Milestone 2 — Raft Consensus & Fault Tolerance

**Soft deadline: November 18, 2026 · Hard deadline: November 20, 2026**

Milestone 2 upgrades the single-node `STANDALONE` system into a full
5-node Raft cluster with leader election, log replication, failure
detection, node recovery, and automated stress testing.

---

## 1. What Changes vs. Milestone 1

| Component | M1 state | M2 target |
|-----------|----------|-----------|
| `raft/node.py` | STANDALONE stub, stubs raise `NotImplementedError` | Full Raft: election, heartbeat, replication |
| `raft/log.py` | Complete, not yet called | In active use by replication pipeline |
| `raft/state_machine.py` | Logic written, not wired | Called from `_apply_committed()` |
| `app/server.py` | Writes go directly to DB | Writes go through `raft.propose()` |
| `docker-compose.yml` | 1 app node | 5 app nodes with `PEERS` wired |
| `tests/` | No tests | Full suite: concurrency, failure, recovery, consistency |

### No changes needed

```
proto/raft.proto         Already complete (RequestVote, AppendEntries)
raft/log.py              Already functional (get / append / truncate_from / last_index)
storage/database.py      raft_log + raft_state tables already created on startup
app/auth, chat, presence, files   No changes — called by state machine, not the HTTP layer
```

---

## 2. Raft Implementation Plan

### 2.1 Persistent State (already stored, not yet read/written)

```python
# raft_state rows:
{ "key": "current_term",  "value": "7" }
{ "key": "voted_for",     "value": "node-3" }   # or "" if not voted this term
```

**Rule**: both fields must be written to disk (committed) **before** responding
to any `RequestVote` or `AppendEntries` RPC, to survive crashes.

---

### 2.2 `RaftNode` — `raft/node.py`

Replace the `NotImplementedError` stubs with real implementations.

#### Constants

```python
ELECTION_TIMEOUT_MIN = 0.150   # seconds
ELECTION_TIMEOUT_MAX = 0.300
HEARTBEAT_INTERVAL   = 0.050   # 50 ms
```

#### `_start_election()`

```python
def _start_election(self):
    self.state     = NodeState.CANDIDATE
    self.term     += 1
    self.voted_for = self.node_id
    self._persist_state()

    last_idx  = self.log.last_index()
    last_term = self.log.get(last_idx).term if last_idx else 0
    votes = 1  # vote for self

    futures = [
        executor.submit(self._request_vote, peer, self.term, last_idx, last_term)
        for peer in self.peers
    ]
    for f in concurrent.futures.as_completed(futures):
        granted, peer_term = f.result()
        if peer_term > self.term:
            self._become_follower(peer_term)
            return
        if granted:
            votes += 1

    if votes >= self._majority() and self.state == NodeState.CANDIDATE:
        self._become_leader()
```

#### `request_vote()` — handles incoming `RequestVote` RPC

```python
def request_vote(self, term, candidate_id, last_log_index, last_log_term):
    if term > self.term:
        self._become_follower(term)

    grant = (
        term >= self.term
        and (self.voted_for in (None, candidate_id))
        and self._log_up_to_date(last_log_index, last_log_term)
    )
    if grant:
        self.voted_for = candidate_id
        self._persist_state()
        self._reset_election_timer()
    return grant, self.term
```

#### `append_entries()` — handles incoming `AppendEntries` RPC (also heartbeat)

```python
def append_entries(self, term, leader_id, prev_log_index, prev_log_term,
                   entries, leader_commit):
    if term < self.term:
        return False, self.term

    self._reset_election_timer()
    self._become_follower(term)
    self.leader_id = leader_id

    # Log consistency check
    if prev_log_index > 0:
        prev = self.log.get(prev_log_index)
        if not prev or prev.term != prev_log_term:
            return False, self.term

    # Append (truncate on conflict)
    for e in entries:
        existing = self.log.get(e.index)
        if existing and existing.term != e.term:
            self.log.truncate_from(e.index)
        self.log.append(e)

    # Advance commit
    if leader_commit > self.commit_index:
        self.commit_index = min(leader_commit, self.log.last_index())
        self._apply_committed()

    return True, self.term
```

#### `propose()` — called by app server on writes (leader only)

```python
def propose(self, entry: LogEntry) -> dict:
    """
    Append to log, replicate to majority, apply to state machine.
    Blocks until committed. Returns state machine result.
    """
    assert self.state == NodeState.LEADER
    entry.index = self.log.last_index() + 1
    entry.term  = self.term
    self.log.append(entry)

    event = threading.Event()
    self._pending[entry.index] = event

    self._send_appends()          # parallel RPCs to all peers
    event.wait(timeout=5.0)       # wait for majority ack → commitIndex advance

    return self._sm_results.get(entry.index)
```

#### Leader loop (heartbeats + replication)

```python
def _leader_loop(self):
    while self.state == NodeState.LEADER:
        self._send_appends()
        time.sleep(HEARTBEAT_INTERVAL)

def _send_appends(self):
    for peer in self.peers:
        threading.Thread(target=self._replicate_to, args=(peer,), daemon=True).start()

def _replicate_to(self, peer):
    next_idx = self.next_index.get(peer, self.log.last_index() + 1)
    entries  = self.log.get_range(next_idx, self.log.last_index())
    prev_idx = next_idx - 1
    prev_term = self.log.get(prev_idx).term if prev_idx else 0

    stub    = self._get_raft_stub(peer)
    success = stub.AppendEntries(term=self.term, leader_id=self.node_id,
                                  prev_log_index=prev_idx, prev_log_term=prev_term,
                                  entries=entries, leader_commit=self.commit_index)
    if success:
        self.match_index[peer] = self.log.last_index()
        self.next_index[peer]  = self.log.last_index() + 1
        self._try_advance_commit()
    else:
        self.next_index[peer] = max(1, self.next_index[peer] - 1)
```

---

### 2.3 Add `RaftService` gRPC server — `app/server.py`

Add a second gRPC server on the Raft port (`PORT + 1000`):

```python
import raft_pb2_grpc
from raft.node import RaftNode
from raft.log import LogEntry

class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, raft: RaftNode):
        self.raft = raft

    def RequestVote(self, request, context):
        granted, term = self.raft.request_vote(
            request.term, request.candidate_id,
            request.last_log_index, request.last_log_term,
        )
        return raft_pb2.RequestVoteResponse(term=term, vote_granted=granted)

    def AppendEntries(self, request, context):
        entries = [LogEntry(index=e.index, term=e.term,
                            command_type=e.command_type, payload=e.payload,
                            request_id=e.request_id)
                   for e in request.entries]
        success, term = self.raft.append_entries(
            request.term, request.leader_id,
            request.prev_log_index, request.prev_log_term,
            entries, request.leader_commit,
        )
        return raft_pb2.AppendEntriesResponse(
            term=term, success=success,
            match_index=self.raft.log.last_index(),
        )

# In serve():
raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
raft_pb2_grpc.add_RaftServiceServicer_to_server(RaftServicer(raft), raft_server)
raft_server.add_insecure_port(f"[::]:{PORT + 1000}")
raft_server.start()
```

---

### 2.4 Change the write path — `app/server.py`

**Current (M1):**
```python
def SendMessage(self, request, context):
    ok, msg, err = self.chat.send_message(...)   # direct DB write
```

**Target (M2):**
```python
def SendMessage(self, request, context):
    sess = self._require_auth(request.token, context)
    if not sess:
        return chat_pb2.SendMessageResponse(success=False)

    if not self.raft.is_leader():
        return self._forward_to_leader(request, "SendMessage")

    import json
    entry = LogEntry(
        index        = 0,            # set by propose()
        term         = 0,            # set by propose()
        command_type = "SEND_MESSAGE",
        payload      = json.dumps({
            "channel_id":        request.channel_id,
            "sender_id":         sess["user_id"],
            "content":           request.content,
            "client_request_id": request.client_request_id,
            "file_id":           request.file_id or "",
        }),
        request_id   = request.client_request_id,
    )
    result = self.raft.propose(entry)   # blocks until majority commits
    if result and result.get("ok"):
        return chat_pb2.SendMessageResponse(success=True, message=result["message"])
    return chat_pb2.SendMessageResponse(success=False, error="Raft propose failed")
```

Apply the same pattern to: `CreateChannel`, `DeleteChannel`, `JoinChannel`,
`LeaveChannel`, `UploadFile`.

Read-only RPCs (`GetMessages`, `GetPresence`, `ListChannels`, `DownloadFile`,
`GetNodeStatus`, LLM proxies) are served from local state — no Raft needed.

---

### 2.5 Follower request forwarding

```python
def _forward_to_leader(self, request, method_name: str):
    """Client connected to a follower. Forward write to leader."""
    lid = self.raft.leader_id
    if not lid:
        context.abort(grpc.StatusCode.UNAVAILABLE, "No leader currently known")
        return None
    stub = self._get_chat_stub(lid)
    return getattr(stub, method_name)(request)
```

Clients can connect to **any** node — no leader discovery needed.

---

### 2.6 Wire the State Machine — `raft/state_machine.py`

Already written in M1. Wire it into `_apply_committed()` in `raft/node.py`:

```python
def _apply_committed(self):
    while self.last_applied < self.commit_index:
        self.last_applied += 1
        entry  = self.log.get(self.last_applied)
        result = self.state_machine.apply(entry)    # calls chat/auth/files manager
        # Wake up the waiting propose() call
        if self.last_applied in self._pending:
            self._sm_results[self.last_applied] = result
            self._pending.pop(self.last_applied).set()
```

---

## 3. Docker Compose — 5-Node Cluster

Uncomment the node blocks in [`docker-compose.yml`](../docker-compose.yml):

```yaml
app-node-2:
  build: {context: ., dockerfile: docker/Dockerfile.app}
  environment:
    NODE_ID:           "node-2"
    PORT:              "50052"
    DB_PATH:           "/data/chat.db"
    FILE_STORAGE_PATH: "/data/files/"
    LLM_SERVER:        "llm-server:50060"
    PEERS:             "app-node-1:50051,app-node-3:50053,app-node-4:50054,app-node-5:50055"
  volumes: [node2-data:/data]
  ports:   ["50052:50052"]
  networks: [chat-net]
```

Repeat for `app-node-3` (50053), `app-node-4` (50054), `app-node-5` (50055).  
Update `PEERS` on `app-node-1` to include all other nodes.

Also add volumes:
```yaml
volumes:
  node1-data:
  node2-data:
  node3-data:
  node4-data:
  node5-data:
```

---

## 4. Test Scripts to Build (`tests/`)

### `tests/concurrent_clients.py` — Scenario A

```
100 client threads × 10 messages each → 1 channel

Assertions:
  - Total messages committed = 1000 (no loss)
  - No duplicates (client_request_id idempotency)
  - All 5 replicas show same message count
  - Message order same on all replicas (by raft_log_index)
```

### `tests/kill_leader.py` — Scenario B

```
1. GetNodeStatus on all 5 nodes → identify leader
2. Start background write traffic (10 msg/s)
3. docker stop <leader_container>
4. Wait ≤ 600 ms → new leader elected
5. Continue traffic
6. docker start <leader_container>
7. Wait for log catch-up
8. consistency_check.py → 0 differences
```

### `tests/follower_recovery.py` — Scenario C

```
1. Kill one follower
2. Send 50 more messages
3. docker start <follower>
4. Wait for catch-up (watch logs for LOG_CATCHUP_DONE)
5. consistency_check.py → 0 differences
```

### `tests/multiple_failures.py` — Scenario D

```
5-node cluster:
  Kill 2 → cluster continues (quorum = 3 of 5 remaining)
  Kill 1 more → writes blocked (quorum lost: 2 of 5)
  Revive 1 → writes resume
  Revive 2 → full cluster
```

### `tests/consistency_check.py`

```python
"""
Query GetMessages on all 5 nodes for each channel.
Compare:
  - Total message count
  - message_id sequence
  - raft_log_index sequence
Report any divergence.
"""
```

---

## 5. Observability Checklist

Make sure the following events are clearly emitted in logs for the demo:

```
[RAFT] BECAME_CANDIDATE          term=8  node=node-3
[RAFT] REQUEST_VOTE_SENT         to=node-2  term=8
[RAFT] VOTE_GRANTED              from=node-2  term=8
[RAFT] BECAME_LEADER             term=8  node=node-3
[RAFT] HEARTBEAT_SENT            to=4 peers
[RAFT] ENTRY_APPENDED            index=104  term=8  cmd=SEND_MESSAGE
[RAFT] MATCH_INDEX_UPDATED       peer=node-2  match=104
[RAFT] COMMIT_INDEX_ADVANCED     index=104
[RAFT] STATE_MACHINE_APPLIED     index=104  cmd=SEND_MESSAGE
[RAFT] LEADER_LOST               last_seen=node-1  new_term=8
[RAFT] LOG_CATCHUP_START         from_index=87  to_index=104
[RAFT] LOG_CATCHUP_DONE          entries_applied=17
```

---

## 6. Key Properties to Demonstrate

| Property | Demo scenario |
|----------|---------------|
| **Leader election** | Kill leader → watch election in logs, new leader within 300 ms |
| **Log replication** | Send 10 messages → all 5 replicas show identical `raft_log_index` values |
| **Concurrent safety** | 100 threads × 10 messages → no duplicates, same order on all replicas |
| **Fault tolerance (minority)** | 2 of 5 nodes dead → cluster still operates |
| **Quorum loss** | 3 of 5 nodes dead → writes blocked (returns UNAVAILABLE) |
| **Follower recovery** | Restart dead node → log catches up, consistency_check passes |
| **Idempotency** | Client retry after leader change → exactly-once delivery |
| **Follower forwarding** | Client → follower → leader → commit → response |

---

## 7. New Files to Create

```
raft/
  election.py        # Election timer, parallel RequestVote RPCs
  replication.py     # matchIndex, nextIndex, try_advance_commit logic

tests/
  concurrent_clients.py
  kill_leader.py
  follower_recovery.py
  multiple_failures.py
  consistency_check.py

scripts/
  start_cluster.sh   # docker compose up -d all 5 nodes
  kill_leader.sh     # detect + stop leader container
  stress_test.py     # run concurrent_clients.py + kill_leader.py together
```

## 8. Files to Modify

```
raft/node.py          # Fill all NotImplementedError stubs
app/server.py         # Route writes through raft.propose(); add RaftServicer
docker-compose.yml    # Uncomment app-node-2..5 + PEERS + volumes
```
