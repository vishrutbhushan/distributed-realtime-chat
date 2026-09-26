# Walkthrough — Milestone 1

Implementation notes for the M1 application. The current, self-checking
recording flow and exact commands are in [MILESTONE1_DEMO.md](MILESTONE1_DEMO.md).
This document's fixed example outputs are illustrative; local-model output is
generated at runtime.

---

## 1. Running the System

### Full automated demo

```bash
docker compose build
docker compose up --abort-on-container-exit --exit-code-from client-runner
```

The app and LLM services start independently; the client waits for the app:

```
llm-server     → loads the pinned local model, then passes a gRPC readiness check
app-node-1     → starts independently so chat works when AI is unavailable
client-runner  → starts after app-node-1 is healthy, runs the demo, exits 0 on PASS
```

`--abort-on-container-exit` brings the cluster down cleanly once client-runner
exits with code 0.

### Interactive mode

```bash
# Keep cluster alive
docker compose up -d llm-server app-node-1

# Run CLI locally (requires grpcio, grpcio-tools, protobuf and generated stubs)
.\scripts\generate_proto.ps1
python client/client.py --server localhost:50051
```

---

## 2. Server Startup Sequence

When `app-node-1` starts, log output looks like this:

```
[18:36:17] NODE=node-1 INFO [DB] Initialised at /data/chat.db
[18:36:17] NODE=node-1 INFO [RAFT] Node node-1 started in NodeState.STANDALONE mode  peers=[]
[18:36:17] NODE=node-1 INFO [AUTH] Created user=admin role=ADMIN
[18:36:17] NODE=node-1 INFO [SEED] admin / admin123 created
[18:36:17] NODE=node-1 INFO [AUTH] Created user=alice role=USER
[18:36:17] NODE=node-1 INFO [AUTH] Created user=bob role=USER
[18:36:17] NODE=node-1 INFO [CHAT] Created channel #general
[18:36:17] NODE=node-1 INFO [CHAT] Created channel #aos-project
[18:36:17] NODE=node-1 INFO [CHAT] Created channel #testing
[18:36:17] NODE=node-1 INFO [SERVER] Listening on [::]:50051  (node_id=node-1  state=NodeState.STANDALONE)
```

What happens under the hood:
1. **`Database.__init__`** — runs `_init_schema()` (idempotent `CREATE TABLE IF NOT EXISTS`)
2. **`RaftNode.__init__`** — reads `raft_state` from SQLite; defaults `term=0 state=STANDALONE`
3. **`ChatServicer._seed_defaults()`** — creates admin/alice/bob and three channels if they don't exist
4. **`PresenceManager.__init__`** — starts background sweep thread (30 s interval)
5. **gRPC server** — binds to `[::]:{PORT}` with 16-worker `ThreadPoolExecutor`

On subsequent restarts the seed is **idempotent** — `INSERT OR IGNORE` / `UNIQUE` constraints
prevent duplicates so the DB state is clean.

---

## 3. Demo Step-by-Step

### Step 1 — Node Status

```
node_id      = node-1
state        = STANDALONE
term         = 0
commit_index = 0
```

`GetNodeStatus` asks `RaftNode.status()`. In M1 this always returns
`state=STANDALONE, term=0, leader_id=""`. In M2 this will show the real
Raft term, current leader, and commit index.

---

### Steps 2–4 — Login

```
admin token = ...e80f6674  role = ADMIN
alice token = ...83f8aada
bob   token = ...595733b2
```

Each `Login` call internally:
1. Queries `users` for `username` + `SHA-256(password)` match
2. Generates a UUID4 token
3. Inserts into `sessions` with `expires_at = now + 86400`
4. Does `INSERT OR REPLACE INTO presence ... 'ONLINE'`
5. Returns `(success=True, token, user_id, role)`

All subsequent RPCs carry this token. The server calls `validate_token()` on
every request and refreshes the `presence.last_seen` timestamp as a side-effect.

---

### Step 5 — List Channels

```
#aos-project  (members: 1)
#general      (members: 1)
#testing      (members: 1)
```

These three channels were auto-seeded by `_seed_defaults()` on startup.
The query does a `LEFT JOIN channel_members GROUP BY channel_id` to include
member count. Admin is the only member at this point.

---

### Steps 6–8 — Create Channel + Join

```
Admin creates #milestone1-demo    success = True
Alice joins #milestone1-demo      Joined channel
Bob   joins #milestone1-demo      Joined channel
```

**CreateChannel**: ADMIN-only. Validates token role, inserts into `channels`,
then auto-inserts the creator into `channel_members`.

**JoinChannel**: `INSERT OR IGNORE INTO channel_members` — calling it twice is safe.

---

### Step 9 — Sending Messages

```
[alice] Hey everyone! Are we still meeting at 5?  (id: ...ee4a9d1f  ts: 1788287783)
[bob]   Yes! Meeting at 5 PM — don't forget the slides.
[alice] Got it. I'll handle the deployment demo.
[bob]   The CI pipeline failed again — anyone know why?
[alice] Let me check the logs.
```

Each `SendMessage` call:
1. `validate_token` → updates presence heartbeat
2. Checks `client_request_id` in `messages` → not found (fresh send)
3. `INSERT INTO messages` — `timestamp` is **millisecond** epoch (`time.time() * 1000`)
4. `raft_log_index = 0` in M1 (set by Raft state machine in M2)
5. `_notify_subscribers(channel_id, msg)` pushes to any active `StreamMessages` queues

---

### Step 10 — Idempotency

```
r1.message_id = ...13ca0c84
r2.message_id = ...13ca0c84
Same ID? True  ← should be True ✓
```

The client generates one UUID and sends the same `client_request_id` twice.

First call path: `client_request_id` not in DB → `INSERT` → return new row.  
Second call path: `client_request_id` found → return existing row unchanged.

Server log shows:
```
[CHAT] Idempotent replay request_id=bfb1f318-67e7-41b1-beab-5cf57a3503a6
```

This guarantees **exactly-once visible delivery** even when clients retry due to
network timeouts — a fundamental requirement for distributed reliability.

---

### Step 11 — Message History

```
[alice] Hey everyone! Are we still meeting at 5?
[bob]   Yes! Meeting at 5 PM — don't forget the slides.
...
[alice] This message is sent twice (idempotency test)
```

`GetMessages` fetches the latest N rows `ORDER BY timestamp DESC LIMIT N`,
then reverses them to chronological order. In M2, messages will also carry a
`raft_log_index` so order is deterministic across all replicas regardless of
clock skew.

---

### Step 12 — Presence

```
bob    →  ONLINE
admin  →  ONLINE
alice  →  ONLINE
```

All three users recently sent RPCs, so their `last_seen` is within the 60 s
threshold. The background `PresenceManager` sweep thread runs every 30 s and
marks anyone stale as `OFFLINE`.

`GetPresence` queries presence joined with `channel_members` for the given
channel — only members of the channel are returned.

---

### Step 13 — File Upload and Download

```
success  = True  msg = File uploaded
file_id  = ...2b1c829f
size     = 147 bytes
download = OK
content  = This is a sample presentation file for the demo. T...
```

**Upload** flow:
1. `len(data) > 50 MB` → reject early
2. Write bytes to `/data/files/<uuid>` (Docker volume `node1-data`)
3. `INSERT INTO files` (metadata row)
4. Return `file_id`

**Download** flow:
1. `SELECT * FROM files WHERE file_id = ?`
2. `open(storage_location, 'rb')` → return bytes

In M2, file metadata will be replicated through the Raft log so every node
can serve downloads. The binary blob is either synced separately or fetched
from the owning node via an internal RPC.

---

### Step 14 — LLM Smart Replies

```
success = True
[1] Got it, thanks!
[2] Will do.
[3] Sounds good — let me know if you need anything else.
```

Call chain:
```
client-runner
  → ChatService.GetSmartReplies (app-node-1:50051)
      → LLMService.GetSmartReplies (llm-server:50060)
              → local llama.cpp inference
          ← three context-grounded suggestions
      ← SmartReplyResponse
  ← SmartReplyResponse (proxied)
```

The app server is a **transparent proxy** — clients never talk to the LLM server
directly. This lets the LLM server be replaced (different model, GPU hardware,
external API) without any client changes.

The real model is configured during image build and loaded before the LLM service
opens its gRPC port. See [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md) for
its source revision and license.

---

### Step 15 — Conversation Summary

The CLI prints a runtime-generated summary of recent chat topics, decisions,
action items, and open questions.

The app server fetches messages from its **local DB** (no extra network RPC),
formats them as `"username: content"` strings, and sends them to the LLM server
in a `SummarizeConversation` request.

The local model produces a concise summary from the recent messages.

---

### Step 16 — Context Suggestion

The CLI prints a runtime-generated next-step suggestion based on recent messages.

The local model receives recent messages and the current user's name, then
generates one concrete next step grounded in that conversation.

---

### Steps 17–18 — Admin User Management + Logout

The demo creates and removes a run-specific temporary account, verifies that it
cannot log in afterward, then logs out its remaining sessions.

`AddUser` is ADMIN-only; any USER token is rejected with `PERMISSION_DENIED`.

`Logout` deletes the session row from `sessions` and sets presence to `OFFLINE`.
Subsequent calls with that token will fail `validate_token` and return
`UNAUTHENTICATED`.

---

## 4. Log Reference

All server logs follow the pattern:

```
[HH:MM:SS] NODE=<node_id> LEVEL [MODULE] Message
```

| Log line | Meaning |
|----------|---------|
| `[DB] Initialised at /data/chat.db` | Schema created/verified on startup |
| `[RAFT] Node node-1 started in NodeState.STANDALONE` | M1 mode, no peers |
| `[SEED] admin / admin123 created` | Default user created (first start only) |
| `[SERVER] Listening on [::]:50051` | gRPC server ready to accept connections |
| `[RPC] Login user=admin` | Incoming Login RPC received |
| `[AUTH] Login user=admin` | Credentials verified, token issued |
| `[CHAT] Created channel #general` | Channel inserted into DB |
| `[CHAT] Idempotent replay request_id=...` | Duplicate request detected + returned |
| `[FILES] Uploaded demo-notes.txt (...)` | File stored on disk |
| `[LLM] SmartReplies channel=milestone1-demo` | LLM server received smart reply request |
| `[LLM] Summarize channel=milestone1-demo msgs=6` | LLM summarize request |
| `[PRESENCE] Sweep error: ...` | Background sweep encountered a DB error |

---

## 5. Interactive CLI Commands

```bash
python client/client.py --server localhost:50051
```

```
login <user> <pass>    Log in and obtain a session token
logout                 Invalidate current token
status                 Show node_id, state, term, commit_index
channels               List all channels with IDs and member counts
join <channel_id>      Join a channel
leave <channel_id>     Leave a channel
use <channel_id>       Set active channel for subsequent commands
send <text>            Send a message to the active channel
history [n]            Retrieve last n messages (default 20)
presence               Show ONLINE/OFFLINE status for channel members
smartreply             Get 3 LLM reply suggestions for the last message
summarize              Bullet-point summary of the active channel
suggest                Context-aware next-action suggestion
files                  List uploaded files in the active channel
help                   Show this list
quit                   Exit the client
```

---

## 6. StreamMessages (Real-time Push)

The `StreamMessages` RPC is a **server-side streaming** gRPC call.
When a client opens a stream on a channel:

1. The server calls `chat.subscribe(channel_id, queue)` — adding a `queue.Queue` to the channel's subscriber list
2. A loop runs `queue.get(timeout=1.0)` and yields `MessageEvent` protos
3. When another client sends a message, `_notify_subscribers()` does `queue.put_nowait(msg)` for every subscriber
4. When the client disconnects, `context.is_active()` returns `False` and `chat.unsubscribe()` cleans up

This gives real-time push with no polling. In M2 the notification will come
from the Raft state machine's `apply()` instead of the direct DB insert.

---

## 7. File Layout After Running

```
/data/                    ← Docker volume node1-data
  chat.db                 ← SQLite database (all tables)
  files/
    <uuid>                ← uploaded file blob (no extension)
    <uuid>
    ...
```

```
/app/                     ← inside app-node-1 container
  generated/
    chat_pb2.py
    chat_pb2_grpc.py
    raft_pb2.py
    raft_pb2_grpc.py
    llm_pb2.py
    llm_pb2_grpc.py
  app/
  storage/
  raft/
  client/
```
