# Milestone 2 — Raft Consensus & Distributed Replication Roadmap

## 1. Goal

Upgrade the system from a single standalone node to a **5-node distributed cluster** providing:
- **Raft Consensus Protocol**: Leader election, log replication, failure detection, and term management.
- **Strong Consistency**: Client writes (`SendMessage`, `CreateGroup`, `UpdateGroup`, `UploadFile`) must reach a majority quorum across the cluster before being committed to the state machine and local SQLite databases.
- **Fault Tolerance & Node Recovery**: Any 2 nodes in the 5-node cluster can crash, pause, or partition without data loss or downtime. Recovered nodes automatically catch up via `AppendEntries`.
- **Client Transparency**: Clients can connect to any node. If a client connects to a follower, write operations are transparently redirected or forwarded to the current Raft leader.

---

## 2. Cluster Deployment Blueprint (5 Nodes + 1 LLM Server)

```text
                               ┌──────────────────────────┐
                               │       LLM SERVER         │
                               │  port 50060 (Standalone) │
                               └────────────▲─────────────┘
                                            │ gRPC
              ┌─────────────────────────────┼─────────────────────────────┐
              │                             │                             │
        ┌─────▼─────┐                 ┌─────▼─────┐                 ┌─────▼─────┐
        │  Node 1   │◄═══════════════►│  Node 2   │◄═══════════════►│  Node 3   │
        │   RAFT    │                 │   RAFT    │                 │   RAFT    │
        │  LEADER   │                 │ FOLLOWER  │                 │ FOLLOWER  │
        │ gRPC:50051│                 │ gRPC:50052│                 │ gRPC:50053│
        │ Web :8000 │                 │ Web :8002 │                 │ Web :8003 │
        └─────▲─────┘                 └─────▲─────┘                 └─────▲─────┘
              ▲                             ▲                             ▲
              ║                             ║                             ║
              ║   Raft RPCs (raft.proto)    ║                             ║
              ║                             ║                             ║
              ▼                             ▼                             ▼
        ┌───────────┐                 ┌───────────┐                       ║
        │  Node 4   │◄═══════════════►│  Node 5   │◄══════════════════════╝
        │   RAFT    │                 │   RAFT    │
        │ FOLLOWER  │                 │ FOLLOWER  │
        │ gRPC:50054│                 │ gRPC:50055│
        │ Web :8004 │                 │ Web :8005 │
        └───────────┘                 └───────────┘
```

---

## 3. Pending Changes & Modules to Implement

### 3.1 Raft RPC Handlers (`raft/node.py` & `proto/raft.proto`)
The proto definition in `proto/raft.proto` already defines the canonical Raft RPC signatures:
```protobuf
service RaftService {
  rpc RequestVote(RequestVoteArgs)       returns (RequestVoteReply);
  rpc AppendEntries(AppendEntriesArgs)   returns (AppendEntriesReply);
}
```
In `raft/node.py`, the following will be implemented:
1. **Randomized Election Timer**:
   - Each follower maintains an election timer between 150 ms – 300 ms.
   - If no heartbeat is received before timeout, follower transitions to `CANDIDATE`, increments `currentTerm`, votes for itself, and broadcasts `RequestVote` RPCs to all peers.
2. **Leader Heartbeat & AppendEntries**:
   - The elected leader continuously sends periodic empty `AppendEntries` heartbeats (every 50 ms) to keep peers from timing out.
3. **Log Replication Quorum**:
   - When a client sends a write (e.g. `SendMessage`), the leader appends the entry to its `raft_log` table.
   - The leader sends `AppendEntries` containing the entry to all followers.
   - When a majority (3 of 5 nodes) acknowledge the write, the leader advances `commitIndex` and applies the entry.

### 3.2 State Machine Execution (`raft/state_machine.py`)
In Milestone 1, the managers (`AuthManager`, `ChatManager`, `FileManager`) write directly to SQLite.
In Milestone 2:
- Write requests will produce a `LogEntry(index, term, command_type, payload, request_id)`.
- The `StateMachine.apply(entry)` method will be invoked exclusively when an entry is committed by Raft quorum.
- The command types to replicate:
  - `SEND_DM`
  - `SEND_GROUP_MESSAGE`
  - `CREATE_GROUP`
  - `UPDATE_GROUP`
  - `UPLOAD_FILE_METADATA`
  - `USER_SIGNUP` / `USER_STATUS`

### 3.3 Write Forwarding / Leader Redirection
- When a client issues a write request to a node that is currently in `FOLLOWER` state, the follower can either:
  1. Return `leader_id` and have the client reconnect to the leader, or
  2. Proxy the write RPC to the leader internally and return the leader's response.
- `GetNodeStatus` will report the node's current role (`LEADER`, `FOLLOWER`, `CANDIDATE`), current term, and `leader_id`.

### 3.4 Fault Injection Demos
- **Leader Failure**: Stop `app-node-1` (`docker stop app-node-1`). Followers detect timeout, trigger new election, and elect a new leader.
- **Partition Tolerance**: Pause 1 or 2 followers (`docker pause app-node-3`). The 3 remaining nodes still form a quorum and continue processing messages without downtime.
- **Node Rejoin & Log Catchup**: When `app-node-1` restarts (`docker start app-node-1`), it discovers the higher term, steps down to follower, and brings its log up to date via `AppendEntries`.
