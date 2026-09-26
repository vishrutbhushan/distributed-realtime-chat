# Architecture — Distributed Real-time Chat

## 1. Overview

The M1 deliverable is a standalone application node backed by SQLite. The
architecture diagram below shows the planned multi-node M2 system: Raft
consensus, replicated state, and fault recovery are not active in M1.

```
                    ┌──────────────────────────┐
                    │       LLM SERVER         │
                    │  llm/server.py           │
                    │  port 50060              │
                    │                          │
                    │  • GetSmartReplies       │
                    │  • SummarizeConversation │
                    │  • GetContextSuggestion  │
                    └──────────┬───────────────┘
                               │ gRPC (llm.proto)
                               │
         ┌─────────────────────┼──────────────────────┐
         │                     │                      │
   ┌─────▼──────┐       ┌──────▼─────┐        ┌──────▼──────┐
   │  Node 1    │◄─────►│  Node 2    │◄───────►│  Node 3     │
   │  LEADER    │       │  FOLLOWER  │        │  FOLLOWER   │
   │            │       │            │        │             │
   │ Auth       │       │ Auth       │        │ Auth        │
   │ Chat       │       │ Chat       │        │ Chat        │
   │ Presence   │       │ Presence   │        │ Presence    │
   │ Files      │       │ Files      │        │ Files       │
   │ RaftNode   │       │ RaftNode   │        │ RaftNode    │
   │ SQLite DB  │       │ SQLite DB  │        │ SQLite DB   │
   │ :50051     │       │ :50052     │        │ :50053      │
   └─────▲──────┘       └──────▲─────┘        └──────▲──────┘
         │                     │                      │
         └──────────┬──────────┴──────────────┬───────┘
                    │                         │
              gRPC (chat.proto)         gRPC (chat.proto)
                    │                         │
             ┌──────▼──────┐           ┌──────▼──────┐
             │  Clients    │           │  Clients    │
             │  A / B / C  │           │  D / E / F  │
             └─────────────┘           └─────────────┘
```

**Milestone 1**: single node in `STANDALONE` state (no Raft).  
**Milestone 2**: 5-node cluster with full Raft (leader election + log replication).

---

## 2. Component Map

### 2.1 Application Node

```
app/server.py  (ChatServicer)
│
├── app/auth/manager.py      AuthManager
│     SHA-256 passwords · UUID tokens · 24h TTL
│
├── app/chat/manager.py      ChatManager
│     Channels · Messages · Idempotency · Streaming subscriptions
│
├── app/presence/manager.py  PresenceManager
│     Heartbeat-driven · 60 s offline threshold · background sweep thread
│
├── app/files/manager.py     FileManager
│     Filesystem blob store · SQLite metadata
│
├── raft/node.py             RaftNode
│     M1: STANDALONE (no-op) · M2: full Raft leader election + replication
│
└── storage/database.py      Database
      Thread-local SQLite · WAL mode · full schema
```

### 2.2 LLM Server

```
llm/server.py  (LLMServicer)
│
├── llm/inference.py
│     Loads pinned Qwen2.5 GGUF model before opening the gRPC port
│     CPU inference is serialized and time bounded
│
└── llm/prompts.py
      Prompt templates for smart reply · summarize · context suggestion
```

### 2.3 Client

```
client/client.py
  --demo flag    →  self-checking M1 recording demo with unique run data
  interactive    →  REPL: send / history / smartreply / summarize / ...
```

---

## 3. gRPC Service Definitions

### 3.1 `ChatService` (chat.proto) — client ↔ app node

| RPC | Type | Description |
|-----|------|-------------|
| `Login` / `Logout` | unary | Auth with UUID tokens |
| `CreateChannel` / `DeleteChannel` | unary | Admin only |
| `JoinChannel` / `LeaveChannel` / `ListChannels` | unary | Any authenticated user |
| `SendMessage` | unary | Idempotent via `client_request_id` |
| `GetMessages` | unary | Paginated history |
| `StreamMessages` | server-streaming | Real-time push |
| `GetPresence` / `UpdatePresence` | unary | ONLINE/OFFLINE status |
| `UploadFile` / `DownloadFile` / `ListFiles` | unary | File sharing |
| `AddUser` / `RemoveUser` | unary | Admin only |
| `GetNodeStatus` | unary | node_id, state, term, leader_id, commit_index |
| `GetSmartReplies` | unary | LLM proxy → 3 reply suggestions |
| `SummarizeChannel` | unary | LLM proxy → conversation summary |
| `GetContextSuggestion` | unary | LLM proxy → next-action recommendation |

### 3.2 `RaftService` (raft.proto) — node ↔ node

| RPC | Description |
|-----|-------------|
| `RequestVote` | Candidate asks peers for votes during election |
| `AppendEntries` | Leader replicates entries + sends periodic heartbeats |

### 3.3 `LLMService` (llm.proto) — app node → LLM server

| RPC | Description |
|-----|-------------|
| `GetLLMAnswer` | Generic feature dispatch |
| `GetSmartReplies` | Return 3 reply suggestions |
| `SummarizeConversation` | Bullet-point summary of message list |
| `GetContextSuggestion` | One-sentence next-action recommendation |

---

## 4. Data Model (SQLite)

Each node maintains its own independent SQLite database at `$DB_PATH`.
In M2 all write operations flow through the Raft log so every node's
state machine applies the same sequence of commands.

```sql
-- Application tables
users          (user_id TEXT PK, username TEXT UNIQUE, password_hash, role, created_at)
sessions       (token TEXT PK, user_id, created_at, expires_at)
channels       (channel_id TEXT PK, name TEXT UNIQUE, created_by, created_at)
channel_members(channel_id, user_id, joined_at)          -- PK (channel_id, user_id)
messages       (message_id TEXT PK, channel_id, sender_id, content, timestamp,
                client_request_id TEXT UNIQUE,            -- idempotency guard
                raft_log_index INTEGER DEFAULT 0,
                file_id TEXT)
files          (file_id TEXT PK, filename, owner_id, channel_id,
                storage_location, size_bytes, content_type, uploaded_at)
presence       (user_id TEXT PK, status TEXT, last_seen INTEGER)

-- Raft tables (Milestone 2 — already created, not yet used)
raft_log       (log_index INTEGER PK, term, command_type, payload TEXT, request_id)
raft_state     (key TEXT PK, value TEXT)    -- currentTerm, votedFor
```

**Key index**: `messages(channel_id, timestamp)` for efficient history queries.

---

## 5. Write Path — Milestone 1 (Direct)

```
Client
  │
  │  SendMessage(token, channel_id, content, client_request_id)
  ▼
ChatServicer.SendMessage
  │
  ├─ validate_token(token)          → {user_id, username, role}
  │
  ├─ presence.update_presence()     → mark ONLINE (heartbeat)
  │
  └─ chat.send_message()
        │
        ├─ idempotency check        → if client_request_id exists, return existing
        │
        ├─ INSERT INTO messages     → ms-epoch timestamp, raft_log_index=0
        │
        └─ _notify_subscribers()    → push to StreamMessages queues
```

## 6. Write Path — Milestone 2 (Raft)

```
Client
  │
  ▼
Any node (follower or leader)
  │
  ├─ if FOLLOWER: forward to leader  ─────────────────┐
  │                                                   │
  └─ if LEADER:                      ◄────────────────┘
        │
        ├─ propose(LogEntry)
        │     │
        │     ├─ AppendEntries → followers (parallel RPCs)
        │     │
        │     └─ wait for majority ack
        │
        ├─ advance commitIndex
        │
        ├─ StateMachine.apply(entry)
        │     │
        │     └─ chat.send_message()  → stored + broadcast
        │
        └─ return result to client
```

---

## 7. Idempotency

Every `SendMessage` call carries a `client_request_id` (UUID from the client).  
The `messages` table has a `UNIQUE` index on this column.  
On retry the existing row is returned — no duplicate is inserted.

This is essential for distributed reliability: network timeouts cause clients to
retry; the system guarantees exactly-once visible delivery.

---

## 8. Presence Model

```
Login           → INSERT OR REPLACE presence(user_id, 'ONLINE', now)
Any RPC (auth)  → UPDATE presence SET last_seen = now (via presence.update_presence)
Logout          → UPDATE presence SET status = 'OFFLINE'

Background sweep thread (every 30 s):
    UPDATE presence SET status = 'OFFLINE'
    WHERE status = 'ONLINE' AND last_seen < now - 60
```

---

## 9. Raft State (Milestone 2)

Each `RaftNode` tracks:

| Field | Persisted | Description |
|-------|-----------|-------------|
| `currentTerm` | ✅ SQLite `raft_state` | Monotonically increasing election term |
| `votedFor` | ✅ SQLite `raft_state` | Candidate voted for in current term |
| `log[]` | ✅ SQLite `raft_log` | Sequence of `LogEntry` (index, term, command, payload) |
| `commitIndex` | Memory | Highest log index known to be committed |
| `lastApplied` | Memory | Highest log index applied to state machine |
| `state` | Memory | `FOLLOWER` \| `CANDIDATE` \| `LEADER` |
| `leader_id` | Memory | Current known leader (for client forwarding) |

### Log commands

```
SEND_MESSAGE    { channel_id, sender_id, content, client_request_id }
CREATE_CHANNEL  { name, created_by }
JOIN_CHANNEL    { channel_id, user_id }
LEAVE_CHANNEL   { channel_id, user_id }
DELETE_CHANNEL  { channel_id }
UPLOAD_FILE     { file_id, filename, owner_id, channel_id, storage_location, ... }
```

---

## 10. Docker Compose Layout

```
┌─────────────────────────── chat-net (bridge) ─────────────────────────────┐
│                                                                            │
│  llm-server    :50060   healthcheck → GetLLMAnswer                        │
│                                                                            │
│  app-node-1    :50051   depends_on llm-server (healthy)                   │
│                         healthcheck → GetNodeStatus                        │
│                         volume: node1-data → /data                        │
│                                                                            │
│  client-runner          depends_on app-node-1 (healthy)                   │
│                         runs --demo then exits 0                           │
│                                                                            │
│  # M2 additions (uncomment in docker-compose.yml):                        │
│  app-node-2..5 :50052-55  PEERS wired between all nodes                   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 11. Proto Compilation

Protos are compiled **inside Docker** at build time using:

```dockerfile
RUN python -m grpc_tools.protoc \
    -I ./proto \
    --python_out=./generated \
    --grpc_python_out=./generated \
    ./proto/chat.proto ./proto/raft.proto ./proto/llm.proto
```

Generated stubs land in `/app/generated/` and are imported via
`PYTHONPATH=/app:/app/generated`.

For local development, run [`scripts/generate_proto.ps1`](../scripts/generate_proto.ps1)
(Windows) or [`scripts/generate_proto.sh`](../scripts/generate_proto.sh) (Linux/Mac).
