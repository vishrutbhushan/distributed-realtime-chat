# Distributed Real-Time Chat & Consensus System: Complete Architectural Knowledge Base

> **Overview**: This document captures the complete architectural knowledge, design decisions, consensus mechanics, failure modes, data structures, network protocols, and troubleshooting lessons learned throughout the development and verification of the Distributed Real-Time Chat system.

---

## 1. System Overview & Core Topology

The system is a high-availability, multi-tenant collaboration platform with strict consistency, zero cloud exposure, and sub-second failover.

```
                                  [ Browser Clients ]
                                          │
                                          │ HTTP / SSE (Port 8000)
                                          ▼
                              ┌───────────────────────┐
                              │  Nginx Load Balancer  │
                              │  (Consistent Hashing) │
                              └───────────┬───────────┘
                                          │
                 ┌────────────────────────┼────────────────────────┐
                 ▼                        ▼                        ▼
          ┌─────────────┐          ┌─────────────┐          ┌─────────────┐
          │ app-node-1  │◄────────►│ app-node-2  │◄────────►│ app-node-3  │ ... (6 Nodes)
          │  (Leader)   │  gRPC    │ (Follower)  │  gRPC    │ (Follower)  │
          │ SQLite(WAL) │  :5000   │ SQLite(WAL) │  :5000   │ SQLite(WAL) │
          └──────┬──────┘          └─────────────┘          └─────────────┘
                 │
                 │ gRPC :50060
                 ▼
          ┌─────────────┐          ┌───────────────────────┐
          │ llm-server  │          │   Raft Observatory    │
          │  Qwen 2.5   │          │  (Dashboard on :8088) │
          └─────────────┘          └───────────────────────┘
```

- **Nginx Reverse Proxy / Load Balancer** (`docker/nginx.conf`): Listens on port `8000`. Directs incoming client traffic to app nodes using **Consistent Hashing** on session affinity keys. Buffering is disabled for real-time Server-Sent Events (`proxy_buffering off;`).
- **Application Nodes (`app-node-1` to `app-node-6`)**: Containerized Python microservices running the HTTP REST Gateway, Raft Consensus engine, gRPC server, and a local SQLite database in WAL mode.
- **Dedicated Local Edge LLM (`llm-server`)**: Runs an isolated Qwen 2.5 1.5B (4-bit quantized) model over internal gRPC (port `50060`) for Smart Replies and Summarization with zero cloud leakage.
- **Raft Observatory Cockpit (`dashboard`)**: Real-time telemetry dashboard on port `8088` tracking live terms, voter roles, log indices, subscriber presence, and deep message tracing.

---

## 2. Authentication, Session Tokens & Identity

### Who Creates the Auth Token?
The **`AuthManager`** running inside whichever application node receives the HTTP request:
- Generates a cryptographically random UUID token using `str(uuid.uuid4())`.
- Assigns a 7-day TTL (`SESSION_TTL = 604800` seconds).
- Stores the token in the `sessions` table (`token`, `user_id`, `created_at`, `expires_at`).

### Signup vs. Login: The Architectural Difference

| Feature | **Signup** | **Login** |
| :--- | :--- | :--- |
| **Purpose** | Register a brand new user profile. | Authenticate an existing user and create an active session. |
| **Credential Check** | Checks if `username` already exists. Returns 409 Conflict if taken. | Checks `hashlib.sha256(password)` against stored `password_hash` in local SQLite. |
| **Raft Command** | Proposes a **`SIGNUP`** command to Raft. | Proposes a **`LOGIN`** command to Raft. |
| **Replication Scope** | Replicates `user_id`, `username`, `password_hash`, and initial session token across **all nodes' SQLite databases**. | Replicates the **new session token** and updates `status = 'active'` across all nodes. |
| **Cluster Benefit** | User exists everywhere identically. | Any node can validate the token immediately upon failover without requiring re-login. |

---

## 3. High Availability, Failover & Session Continuity

When an application node crashes or network partitions, the user experience continues without interruption across 3 layers:

### Layer 1: Nginx Auto-Failover
Configured in `docker/nginx.conf`:
```nginx
proxy_next_upstream error timeout http_502 http_503 http_504;
proxy_next_upstream_tries 3;
```
If `app-node-1` crashes mid-request, Nginx automatically shifts the request to the next available healthy node within milliseconds.

### Layer 2: Authentication Continuity (Zero Re-Login)
Because the `sessions` table is replicated cluster-wide via Raft, when a client request reroutes to `app-node-2`, Node 2 performs a local lookup:
```sql
SELECT user_id FROM sessions WHERE token = ? AND expires_at > ?
```
The session token is already in Node 2's local SQLite database. Authentication succeeds without dropping the session.

### Layer 3: SSE Event Stream Auto-Reconnection
1. If the node hosting an SSE connection dies, the browser's stream reader (`web/js/app.js: runEventStream()`) detects stream termination.
2. The client initiates an automatic reconnection loop with backoff (150ms to 1s) passing:
   `GET /api/events?token=...&session_id=tab-...`
3. Nginx directs the connection to an alive node.
4. The new node emits a `READY` event.
5. The client fetches any messages that occurred during the brief disconnect:
   `GET /api/messages?since=<last_timestamp>`

---

## 4. Consistent Hashing & Session Affinity

### Why Consistent Hashing?
In traditional modulo hashing (`hash % N`), if 1 node dies (N drops from 6 to 5), *nearly every user in the cluster gets remapped to a different server*, causing a connection stampede.

In **Consistent Hashing**:
- Nodes and session keys are mapped onto a 360° circular ring.
- A request maps to the closest node clockwise on the ring.
- If Node 3 dies, **only users mapped to Node 3 get shifted to Node 4**. Users on Nodes 1, 2, 5, and 6 remain untouched.

### The Session Key Priority in Nginx:
1. `X-Chat-Session` header (unique per browser tab stored in `sessionStorage`).
2. `?session_id=` query parameter (used by SSE `EventSource`).
3. `chat_session` cookie (browser/RPC fallback).
4. `$request_id` (fallback for new initial requests).

---

## 5. Client Request ID & Exactly-Once Semantics (Idempotency)

- **What it is**: A client-generated UUID (e.g., `user-7f8a9b2c...`) created in JavaScript *before* dispatching a message POST.
- **Why it is needed**: If a user hits Send and the message is committed to the database, but the network drops before the HTTP `200 OK` reaches the browser, the browser's failover logic retries the request on another node.
- **How deduplication works**:
  - The SQLite schema enforces `client_request_id TEXT UNIQUE`.
  - The Raft State Machine checks if the ID already exists before inserting:
    ```sql
    SELECT message_id FROM messages WHERE client_request_id = ?
    ```
  - If already present, it skips the insert and returns the existing message record.
  - **Result**: No duplicate messages, even during abrupt failover retries.

---

## 6. Heartbeats, Presence & Polling Mechanisms

Four distinct timing loops run at different architectural tiers:

```
1. Raft Peer Heartbeats      : [Leader]   ─────(Every ~50ms)─────> [Followers]
2. SSE Keep-Alive Ping       : [App Node] ─────(Every 15-30s)────> [Browser Client]
3. User Presence Sweeper     : [Daemon]   ─────(Every 1s)────────> [Local RAM Table]
4. Dashboard Telemetry Poll  : [Cockpit]  ─────(Every 1-2s)──────> [All Nodes API]
```

1. **Raft Cluster Heartbeats (50ms)**:
   - Leader sends empty `AppendEntries` RPCs to followers.
   - Resets followers' randomized election timers (150ms–350ms) to maintain cluster stability and leadership.
2. **SSE Keep-Alive Pings (15–30s)**:
   - Server writes comment frames (`: ping\n\n`) over the open SSE stream to prevent proxies, firewalls, and routers from terminating idle TCP sockets.
3. **Presence Manager (`app/presence/manager.py`)**:
   - Tracks active user sessions, open SSE streams, and last client activity.
   - Grants a **10-second grace period** (`STREAM_RECONNECT_GRACE`) during page refreshes or Wi-Fi reconnects before marking a user offline.
   - Runs a background sweeper thread every second (`SWEEP_INTERVAL = 1`) updating status in the directory.
4. **Dashboard Polling (1–2s)**:
   - The monitoring server polls `/api/node/state` across nodes to display live terms, commit indices, and active client counts.

---

## 7. SQLite Schema & Dual Write-Ahead Logging (WAL)

### Database Schema (`storage/database.py`)
Each node maintains a complete, independent SQLite database containing 6 core tables:
- **`users`**: `(user_id PK, username UNIQUE, password_hash, status, last_seen, created_at)`
- **`sessions`**: `(token PK, user_id FK, created_at, expires_at)`
- **`groups`**: `(group_id PK, name, created_by FK, created_at)`
- **`group_members`**: `(group_id FK, user_id FK, role, joined_at, PRIMARY KEY(group_id, user_id))`
- **`messages`**: `(message_id PK, chat_type, sender_id FK, recipient_id, group_id, content, file_id, timestamp, read_at, client_request_id UNIQUE)`
- **`files`**: `(file_id PK, filename, file_type, size_bytes, sender_id, chat_type, target_id, storage_path, timestamp)`

### The Dual Write-Ahead Logging (WAL) Architecture:
1. **Raft Distributed WAL (`raft/log.py`)**:
   - Any state change is appended to the Raft disk log *before* replication.
   - Guarantees nodes remember votes, terms, and entries across power cuts.
2. **SQLite Database WAL (`PRAGMA journal_mode=WAL`)**:
   - Database writes append sequentially to a `-wal` file instead of overwriting database pages directly.
   - **Crucial Benefit**: Readers (users fetching history) and Writers (Raft State Machine applying commits) **never lock each other out**.

---

## 8. Network Protocols: gRPC vs. RPC vs. HTTP

| Feature | **HTTP / REST** | **Traditional RPC** | **gRPC** |
| :--- | :--- | :--- | :--- |
| **Used For** | Browser $\leftrightarrow$ Nginx $\leftrightarrow$ App Node | Legacy distributed calls | Node $\leftrightarrow$ Node (Raft), Node $\leftrightarrow$ LLM |
| **Data Format** | JSON (Text, bulky, slow to parse) | XML / JSON | **Protocol Buffers (Protobuf)**: Compact binary wire format |
| **Transport** | HTTP/1.1 | Custom TCP sockets | **HTTP/2**: Binary multiplexing over 1 persistent socket |
| **Contract** | Loose / Documentation | Varies | Strict compile-time schema via `.proto` files |
| **Speed** | Baseline | Moderate | **3x to 10x faster**, minimal CPU overhead |

### Protocol Buffers (`.proto`)
- Field numbers (e.g., `string username = 1;`) are **binary field tags**, not default values. On the network wire, the string key name `"username"` is replaced by a 1-byte integer tag, drastically shrinking payload size.

---

## 9. Raft Consensus Mechanics: Deep Dive

### We Do NOT Use 2PC or 3PC:
- **2-Phase Commit (2PC)**: Requires **100% unanimous agreement**. If 1 node fails, the whole cluster blocks. Zero fault tolerance.
- **Raft**: Requires only a **Strict Majority Quorum** ($\lfloor N/2 \rfloor + 1$). In our 6-node cluster, **4 nodes agree = commit**. 2 nodes can be dead and the system never stalls.

### Split-Brain Immunity
- To commit an entry or elect a leader, a partition must gather at least 4 nodes.
- In any 6-node cluster, two groups of 4 nodes **must overlap by at least 2 nodes** ($4 + 4 = 8 > 6$).
- Therefore, it is mathematically impossible to have two simultaneous majorities or two active leaders committing entries.

### The Lifecycle of a Write & Commit:
1. **Append to Log (Staging Area)**:
   - When a client sends a message, the Leader writes the entry to its Raft log and replicates it via `AppendEntries` containing the full payload.
   - Followers write the entry to their Raft log on disk, but **do not apply it to SQLite yet**.
2. **Quorum Acknowledgment**:
   - Followers reply `entry_appended = True`.
   - The Leader counts ACKs: 4 out of 6 nodes have persisted the entry.
3. **Commit & State Machine Application**:
   - The Leader updates `commit_index = 42` and applies the entry to its SQLite database.
   - The Leader notifies followers on the very next RPC or heartbeat (`leader_commit_index = 42`).
   - Followers see the new commit index, retrieve entry 42 from their log, and execute `INSERT INTO messages` in SQLite.

### Leader Election & Recovery:
- **`last_log_index`**: Stored in the on-disk Raft WAL. Tracks the newest entry a node holds.
- **`nextIndex`**: Stored in the Leader's RAM. Tracks which log index to send next to each follower.
- **When a failed node returns**:
  - The returning node rejoins as a Follower with an older `last_log_index` (e.g., 20 while Leader is at 45).
  - The Leader decrements `nextIndex` for that follower until finding the match point, then streams all missing entries (21–45).
  - The follower applies them in order, catching up completely with zero data loss.

---

## 10. Local LLM: Smart Replies & Conversation Summarization

### Dedicated Container Architecture
- Runs Qwen 2.5 1.5B (4-bit quantized) in an isolated `llm-server` container on port `50060`.
- App nodes invoke it over gRPC (`GetSmartReplies`, `SummarizeConversation`).

### Lessons Learned & Production Fixes:
1. **Smart Reply Limits & Formatting**:
   - Enforced maximum **10 words per reply** to maintain quick-tap chat usability.
   - Structured JSON schema validation requiring exactly 3 distinct reply options.
   - Replaced rigid domain-specific few-shot prompts with generalized workplace/team examples to prevent biased or repetitive responses.
2. **Summarization Perspective Anchoring**:
   - Summaries must recap the conversation from the requesting user's perspective (`Summary for {current_user}:`).
   - Handled transcript parsing bugs where user names were echoed rather than synthesized.
   - Added regex-based cleanup fallbacks to prevent raw markdown leaks or empty responses.

---

## 11. Presentation & Demo Deck Styling Principles

When aligning the presentation deck (`distributed-realtime-chat-demo.pptx`) with the master design system:
- **Title Slide**: Full-bleed background image with bottom Dark Navy (`#0F172A`) container and project emblem.
- **Content Slides**: Pure white background (`#FFFFFF`) with the signature tricolor accent bar (Amber `#F5AE29` + Sky Blue `#69C2E4` + Red `#E83D26`).
- **Cards & Boxes**: Soft slate rounded cards (`#F8FAFC` fill, `#D7DDE3` border) instead of harsh dark boxes.
- **Service Badges**: Light themed pills (Chat: `#EEF8FB`, Raft: `#FEFAF0`, LLM: `#F6F5FC`, Gateway: `#F1F5F9`).
- **Typography**: Clean Segoe UI typography hierarchy (Titles: Navy Bold, Body: Slate `#334155`, Subtitles: Slate Gray `#64748B`).
