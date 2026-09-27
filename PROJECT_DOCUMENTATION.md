# Technical Architecture and System Design Document
## Distributed Real-time Chat and Collaboration Tool
**Classification**: Advanced Distributed Systems & Operating Systems Project  
**Author**: Engineering Team  
**Date**: September 2026  
**Version**: 1.0.0 (Production / Verified)

---

## 1. Executive Summary & System Objectives

The **Distributed Real-time Chat and Collaboration Tool** is a high-performance, containerized group collaboration and messaging system modeled after modern enterprise messaging suites (such as Google Chat and Slack). 

### 1.1 Core Engineering Principles
1. **Strict gRPC-First Communication**: All inter-service and client-to-server operations use high-performance binary gRPC over HTTP/2 defined with Protocol Buffers (`proto3`). An embedded HTTP Web Gateway translates incoming browser REST calls directly into internal gRPC stubs.
2. **Zero-Default State**: The persistent state starts completely empty. There are zero pre-seeded users, default rooms, or default channels. Every user, session, group, and message is dynamically provisioned through authenticated RPC flows.
3. **Decoupled Local AI Subsystem**: Generative AI assistance (context-aware smart replies and conversation summarization) is completely isolated within a dedicated, CPU-optimized container running a quantized open-weights Large Language Model (Qwen 2.5 1.5B Instruct GGUF). It runs entirely on-premise without external cloud APIs.
4. **Idempotent Message Delivery**: Network interruptions or repeated button clicks can cause duplicate RPC submissions. The persistence engine enforces strict deduplication via client-generated unique transaction tokens (`client_request_id`).
5. **Role-Based Access Control (RBAC)**: Group collaboration enforces strict administrative privilege separation: only group creators (`ADMIN`) can mutate group metadata, add/remove members, or promote peers.
6. **Build-Time Verification Gates**: To guarantee deployability and code correctness, static self-tests run inside the Docker image build process (`docker build`). If any unit test fails, the build halts immediately and no container is generated.

---

## 2. System Architecture & Component Topology

The system is designed as a **multi-node distributed cluster** with a shared decoupled AI service. In accordance with the course roadmap, the architecture defines both the **full multi-server distributed cluster mesh** and the internal anatomy of each individual node.

### 2.1 Multi-Server Distributed Cluster Topology

In the distributed deployment, multiple independent server nodes run concurrently. Clients connect to any available node, while the nodes coordinate state across the cluster using inter-node gRPC:

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│ Client A (Web)  │       │ Client B (Web)  │       │ Client C (CLI)  │
└────────┬────────┘       └────────┬────────┘       └────────┬────────┘
         │ HTTP (:8000)            │ HTTP (:8001)            │ gRPC (:50053)
         ▼                         ▼                         ▼
┌───────────────────┐     ┌───────────────────┐     ┌───────────────────┐
│ Server Node 1     │     │ Server Node 2     │     │ Server Node 3     │
│ [ROLE: LEADER]    │     │ [ROLE: FOLLOWER]  │     │ [ROLE: FOLLOWER]  │
│                   │     │                   │     │                   │
│ • Client Gateway  │     │ • Client Gateway  │     │ • Client Gateway  │
│ • Consensus Engine│     │ • Consensus Engine│     │ • Consensus Engine│
│ • State Machine   │     │ • State Machine   │     │ • State Machine   │
│ • Local DB/Log    │     │ • Local DB/Log    │     │ • Local DB/Log    │
└────────▲──────────┘     └────────▲──────────┘     └────────▲──────────┘
         │                         │                         │
         │◄════════════════════════╪════════════════════════►│
         │   Inter-Node gRPC Consensus Mesh (AppendEntries,  │
         │   RequestVote, Heartbeats, Log Replication)       │
         │                                                   │
         └─────────────────────────┬─────────────────────────┘
                                   │
                                   │ gRPC AI Requests (:50060)
                                   ▼
                ┌─────────────────────────────────────┐
                │ Dedicated LLM Server (`llm-server`) │
                │ • Standalone CPU Inference Engine   │
                │ • Qwen 2.5 1.5B Instruct GGUF Model │
                │ • Smart Replies & Summarization     │
                └─────────────────────────────────────┘
```

#### How Distributed Multi-Server Collaboration Works:
1. **Client Distribution**: Clients can connect to any application node in the cluster (e.g., Client A connects to Node 1 on port 8000, Client B connects to Node 2 on port 8001).
2. **Consensus & Replicated Log**:
   - The cluster elects a **Leader** node.
   - When a client sends a state-mutating command (e.g., sending a message or creating a group) to a **Follower** node, the follower forwards the command to the **Leader**.
   - The Leader assigns a monotonically increasing log index and term, then replicates the command to all follower nodes via `AppendEntries` RPCs.
3. **Commit & State Machine Convergence**: Once a majority (quorum: $\lfloor N/2 \rfloor + 1$) of nodes acknowledge the entry, the leader commits it and applies it to its local SQLite state machine. Followers apply it upon receiving the leader's commit index update, guaranteeing that all nodes converge to identical chat histories.
4. **Decoupled AI Offloading**: The LLM server is kept strictly outside the consensus critical path. Any node can independently query `llm-server:50060` via gRPC without stalling cluster replication.

---

### 2.2 Internal Node Anatomy (Micro-Architecture)

Each individual server node in the cluster is structured as a self-contained monolith:

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Individual Application Node Container (`app-node-N`)                                                   │
│                                                                                                        │
│   ┌────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │ HTTP Web Gateway (Port 8000 + N)                                                               │   │
│   │  • Serves static Single-Page Application assets (`index.html`, `style.css`, `app.js`, `api.js`)│   │
│   │  • Translates client JSON REST requests into binary gRPC calls to the local server engine      │   │
│   └──────────────────────────────────────────────┬─────────────────────────────────────────────────┘   │
│                                                  │                                                     │
│                                                  ▼ In-process / Local gRPC                             │
│   ┌────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Core gRPC Server (`ChatServicer` on Port 50050 + N)                                            │   │
│   │  • ThreadPoolExecutor (16 workers, 64 MB maximum payload buffer)                               │   │
│   │  • Implements client-facing RPCs: Auth, DMs, Groups, File exchange, AI proxy                   │   │
│   │  • Manages inter-node consensus routing and leader forwarding                                  │   │
│   └───────────────┬──────────────────────────────┬────────────────────────────────┬────────────────┘   │
│                   │                              │                                │                    │
│                   ▼                              ▼                                ▼                    │
│   ┌──────────────────────────────┐ ┌──────────────────────────────┐ ┌──────────────────────────────┐   │
│   │ Auth & Presence Managers     │ │ Chat & File Managers         │ │ Synchronized SQLite Layer    │   │
│   │  • Password hashing (SHA256) │ │  • DM/Group message routing  │ │  • Write-Ahead Logging (WAL) │   │
│   │  • Token TTL & heartbeats    │ │  • Idempotency deduplication │ │  • Re-entrant Lock (`RLock`) │   │
│   │  • Background sweeper (20s)  │ │  • Chunked file storage      │ │  • Node Volume: /data/       │   │
│   └──────────────────────────────┘ └──────────────────────────────┘ └──────────────────────────────┘   │
└──────────────────────────────────────────────────┬─────────────────────────────────────────────────────┘
                                                   │
                                                   │ Inter-Container gRPC (Port 50060)
                                                   ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Dedicated LLM Server (`llm-server`) Container                                                          │
│                                                                                                        │
│   ┌────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │ gRPC LLM Service (`LLMService` on :50060)                                                      │   │
│   │  • `GetSmartReplies`: Analyzes chat history & produces 3 grounded reply suggestions            │   │
│   │  • `SummarizeConversation`: Generates structured topic/decision bullet points                  │   │
│   └──────────────────────────────────────────────┬─────────────────────────────────────────────────┘   │
│                                                  │                                                     │
│                                                  ▼                                                     │
│   ┌────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │ CPU Inference Engine (`llama-cpp-python`)                                                      │   │
│   │  • Quantized GGUF Model: Qwen 2.5 1.5B Instruct (4-bit Q4_K_M, ~1 GB)                          │   │
│   │  • High-throughput CPU vector instructions (AVX-512 / ARM NEON)                                │   │
│   │  • Bounded context window (4096 tokens) with sliding-window history preservation               │   │
│   └────────────────────────────────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 2.3 Project Milestones & Deployment Phasing

The project architecture accommodates the two evaluation milestones defined in the course specifications:

| Aspect | Milestone 1 (Current Live Build) | Milestone 2 (Consensus & Scale Phase) |
|---|---|---|
| **Cluster Topology** | 1 Application Node (`app-node-1`) + 1 LLM Server (`llm-server`) | 3–5 Application Nodes (`app-node-1..5`) + 1 LLM Server |
| **State Management** | Local thread-safe SQLite with WAL journal and RLock synchronization | Replicated State Machine via Raft consensus log |
| **Communication** | Client ↔ App Node (gRPC/REST), App Node ↔ LLM (gRPC) | Client ↔ Node (gRPC/REST), Node ↔ Node (Raft RPCs), Node ↔ LLM |
| **Fault Tolerance** | Container restart recovery via persistent Docker volume | Quorum resilience: withstands $\lfloor (N-1)/2 \rfloor$ node failures |
| **AI Capabilities** | Full: Smart replies (3 suggestions) & Summarize on CPU LLM | Full: Replicated chat history passed to LLM on demand |

---

## 3. Communication Protocols & gRPC Schema Design

All application interfaces are defined in Protocol Buffers version 3. The schemas are compiled ahead of time into Python stubs (`generated/chat_pb2.py`, `generated/chat_pb2_grpc.py`, etc.).

### 3.1 Core Services in `proto/chat.proto`

| RPC Method | Request Message | Response Message | Description |
|---|---|---|---|
| `Signup` | `SignupRequest` | `SignupResponse` | Creates user, hashes password, returns initial session token. |
| `Login` | `LoginRequest` | `LoginResponse` | Authenticates credentials, sets status to `active`, issues session token. |
| `Logout` | `LogoutRequest` | `LogoutResponse` | Revokes token, sets status to `inactive`. |
| `ListUsers` | `ListUsersRequest` | `ListUsersResponse` | Returns directory of all registered users with presence indicators and timestamps. |
| `SendDirectMessage` | `SendDirectMessageRequest` | `SendDirectMessageResponse` | Sends 1-on-1 message with deduplication key (`client_request_id`). |
| `GetDirectMessages` | `GetDirectMessagesRequest` | `GetDirectMessagesResponse` | Retrieves chronological message history between two users. |
| `CreateGroup` | `CreateGroupRequest` | `CreateGroupResponse` | Initializes a named group with selected members; assigns creator as `ADMIN`. |
| `UpdateGroup` | `UpdateGroupRequest` | `UpdateGroupResponse` | Performs `RENAME`, `ADD_MEMBER`, `REMOVE_MEMBER`, or `MAKE_ADMIN` (Admin only). |
| `ListGroups` | `ListGroupsRequest` | `ListGroupsResponse` | Lists all groups that the requesting user is a member of. |
| `SendGroupMessage` | `SendGroupMessageRequest` | `SendGroupMessageResponse` | Broadcasts message to all members of a group. |
| `GetGroupMessages` | `GetGroupMessagesRequest` | `GetGroupMessagesResponse` | Fetches historical messages for a specific group. |
| `UploadFile` | `UploadFileRequest` | `UploadFileResponse` | Streams binary file chunk, records metadata, returns `file_id`. |
| `DownloadFile` | `DownloadFileRequest` | `DownloadFileResponse` | Downloads binary file payload by `file_id`. |
| `GetSmartReplies` | `SmartReplyRequest` | `SmartReplyResponse` | Forwards conversation history to LLM server; returns 3 suggestions. |
| `SummarizeChat` | `SummarizeChatRequest` | `SummarizeChatResponse` | Forwards conversation history to LLM server; returns markdown summary. |

### 3.2 AI Service in `proto/llm.proto`

```protobuf
syntax = "proto3";
package llm;

service LLMService {
  rpc GetSmartReplies (SmartReplyRequest) returns (SmartReplyResponse);
  rpc SummarizeConversation (SummarizeRequest) returns (SummarizeResponse);
}

message SmartReplyRequest {
  string request_id = 1;
  repeated string chat_history = 2; // Full conversation strings: "username: message"
  string current_message = 3;       // Most recent trigger message
  string context_title = 4;         // DM or Group name
}

message SmartReplyResponse {
  string request_id = 1;
  repeated string suggestions = 2;  // Exactly 3 unique suggested replies
  bool success = 3;
  string error = 4;
}

message SummarizeRequest {
  string request_id = 1;
  repeated string chat_history = 2;
  string context_title = 3;
}

message SummarizeResponse {
  string request_id = 1;
  string summary = 2;               // Structured markdown summary
  bool success = 3;
  string error = 4;
}
```

---

## 4. Persistence Model & Concurrency Engineering

The persistence layer (`storage/database.py`) is implemented using SQLite with strict guarantees tailored for high-concurrency multi-threaded environments.

### 4.1 Database Schema Definition

```sql
-- 1. Users table: tracks registered identities, credentials, and presence
CREATE TABLE IF NOT EXISTS users (
    user_id       TEXT PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'inactive',  -- 'active' | 'inactive'
    last_seen     INTEGER NOT NULL DEFAULT 0,
    created_at    INTEGER NOT NULL
);

-- 2. Sessions table: token-based authentication with expiration
CREATE TABLE IF NOT EXISTS sessions (
    token      TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- 3. Groups table: collaborative spaces
CREATE TABLE IF NOT EXISTS groups (
    group_id   TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (created_by) REFERENCES users(user_id)
);

-- 4. Group Members table: role-based membership
CREATE TABLE IF NOT EXISTS group_members (
    group_id  TEXT NOT NULL,
    user_id   TEXT NOT NULL,
    role      TEXT NOT NULL DEFAULT 'MEMBER',  -- 'ADMIN' | 'MEMBER'
    joined_at INTEGER NOT NULL,
    PRIMARY KEY (group_id, user_id),
    FOREIGN KEY (group_id) REFERENCES groups(group_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id)  REFERENCES users(user_id)   ON DELETE CASCADE
);

-- 5. Messages table: DMs and group messages with idempotency support
CREATE TABLE IF NOT EXISTS messages (
    message_id        TEXT PRIMARY KEY,
    chat_type         TEXT NOT NULL,               -- 'DM' | 'GROUP'
    sender_id         TEXT NOT NULL,
    recipient_id      TEXT DEFAULT NULL,          -- user_id for DM
    group_id          TEXT DEFAULT NULL,          -- group_id for GROUP
    content           TEXT NOT NULL,
    file_id           TEXT DEFAULT NULL,
    timestamp         INTEGER NOT NULL,
    client_request_id TEXT UNIQUE,                -- Idempotency key
    FOREIGN KEY (sender_id)    REFERENCES users(user_id),
    FOREIGN KEY (recipient_id) REFERENCES users(user_id),
    FOREIGN KEY (group_id)     REFERENCES groups(group_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_messages_dm ON messages(chat_type, sender_id, recipient_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_group ON messages(chat_type, group_id, timestamp);

-- 6. Files table: binary metadata store
CREATE TABLE IF NOT EXISTS files (
    file_id          TEXT PRIMARY KEY,
    filename         TEXT NOT NULL,
    file_type        TEXT NOT NULL,               -- 'image' | 'pdf' | 'other'
    content_type     TEXT NOT NULL DEFAULT 'application/octet-stream',
    size_bytes       INTEGER NOT NULL,
    owner_id         TEXT NOT NULL,
    chat_type        TEXT NOT NULL,               -- 'DM' | 'GROUP'
    target_id        TEXT NOT NULL,               -- recipient user_id or group_id
    storage_location TEXT NOT NULL,
    uploaded_at      INTEGER NOT NULL,
    FOREIGN KEY (owner_id) REFERENCES users(user_id)
);
```

### 4.2 SQLite Concurrency & Deadlock Resolution
In a multithreaded gRPC service, concurrent worker threads frequently read and write to the database simultaneously. SQLite by default uses connection-level locking.

#### The Problem: Thread-Local Connection Deadlock
When each worker thread creates its own `sqlite3.Connection`:
1. **Thread 1** issues an `INSERT` or `UPDATE`, acquiring a SQLite write lock (`RESERVED`).
2. **Thread 2** (e.g., background presence sweeper or a directory poll) executes another query on its own connection. It blocks at the SQLite C-level waiting for Thread 1 to finish.
3. If an application-level lock (`threading.RLock`) was acquired during method entry, Thread 2 holds the Python lock while waiting for SQLite, preventing Thread 1 from acquiring the lock to call `.commit()`.
4. Result: A **cross-layer deadlock** between Python's `threading.RLock` and SQLite's file lock, leading to `sqlite3.OperationalError: database is locked`.

#### The Solution: Synchronized Single Connection
The persistence layer resolves this by unifying all threads through a **single shared SQLite connection** protected by a re-entrant lock (`threading.RLock`):
- `PRAGMA journal_mode=WAL`: Enables Write-Ahead Logging so read queries do not block write commits.
- `PRAGMA busy_timeout=60000`: Sets a 60-second driver timeout before throwing busy errors.
- `PRAGMA synchronous=NORMAL`: Ensures durability while minimizing disk sync latency.
- `check_same_thread=False`: Allows the single connection to be invoked safely across the 16 gRPC pool threads because all database interactions (`execute`, `commit`, `rollback`, `fetchone`) are guarded within `with self._lock:`.

---

## 5. Functional Subsystems & Technical Details

### 5.1 Authentication & Token Lifecycle
- **Registration**: Validates that usernames conform to `^[a-zA-Z0-9_]{3,20}$` and passwords are $\ge 4$ characters. Passwords are hashed with SHA-256 (`hashlib.sha256(password.encode()).hexdigest()`).
- **Session Tokens**: Tokens are random UUIDv4 strings stored in the `sessions` table. Tokens expire after a configurable TTL (default: 24 hours / 86,400 seconds).
- **Session Validation**: Every protected gRPC method invokes `_require_auth(token, context)`. If invalid or expired, the call terminates with `grpc.StatusCode.UNAUTHENTICATED`.

### 5.2 Real-time Presence Tracking
- **Heartbeat & Activity**: Every authenticated API request refreshes the user's `last_seen` timestamp in SQLite.
- **Sweeper Daemon**: A dedicated background thread (`PresenceManager._sweep_loop`) wakes every 5 seconds. If `current_time - last_seen > 20 seconds`, the user's status is updated from `active` to `inactive`.
- **Page Unload Beacon**: When a browser tab is closed or navigated away, a `pagehide` event fires a lightweight non-blocking beacon (`navigator.sendBeacon("/api/logout", ...)`) to immediately mark the user `inactive` without waiting for the timeout.

### 5.3 1-on-1 Direct Messaging & Idempotency
- When a user submits a message, the frontend generates a unique UUIDv4 `client_request_id`.
- The database enforces a `UNIQUE` constraint on `messages.client_request_id`.
- If a network glitch causes the client to resend the payload:
  ```python
  if client_request_id:
      existing = self.db.fetchone("SELECT ... WHERE client_request_id = ?", (client_request_id,))
      if existing:
          return True, dict(existing), ""  # Idempotent replay: returns original message without duplicate insert
  ```

### 5.4 Group Collaboration & Admin Controls
- **Creation**: A user creates a group by specifying a name and selecting member user IDs from a checklist. The creator is automatically inserted into `group_members` with `role = 'ADMIN'`.
- **Admin Verification**: Group modifications (`UpdateGroup`) require verification:
  ```python
  role_row = self.db.fetchone(
      "SELECT role FROM group_members WHERE group_id = ? AND user_id = ?",
      (group_id, requesting_user_id)
  )
  if not role_row or role_row["role"] != "ADMIN":
      return False, {}, "Permission denied: Only group admins can perform this action"
  ```
- **Supported Operations**:
  - `RENAME`: Updates `groups.name`.
  - `ADD_MEMBER`: Inserts new user into `group_members` with role `MEMBER`.
  - `REMOVE_MEMBER`: Deletes user from `group_members` (admins cannot remove themselves).
  - `MAKE_ADMIN`: Promotes an existing `MEMBER` to `ADMIN`.

### 5.5 File Sharing Pipeline
- **Upload Flow**: 
  1. The client submits a multipart POST request with binary file bytes and metadata.
  2. The gateway reads the stream, detects the MIME type, assigns a UUID `file_id`, and writes the binary directly to `/data/files/{file_id}_{filename}`.
  3. Metadata is committed to the `files` table, and the message references `file_id`.
- **Rendering**:
  - **Images** (`image/png`, `image/jpeg`): The UI creates an inline `<img>` tag pointing to `/api/files/download?file_id=...`.
  - **PDFs** (`application/pdf`): The UI renders an interactive document card with the filename, human-readable size (`formatBytes`), and a dedicated download action.

---

## 6. Standalone Local LLM / AI Subsystem

### 6.1 Model & Hardware Optimization
- **Model**: `Qwen 2.5 1.5B Instruct` in GGUF format (`q4_k_m`).
- **Engine**: `llama-cpp-python` (version 0.3.35) compiled for CPU execution.
- **Why Quantized CPU Execution?**
  - High portability: Runs efficiently on developer laptops and cloud servers without requiring NVIDIA GPUs or CUDA drivers.
  - Low memory footprint: Requires only ~1.2 GB of RAM.
  - Fast response: Generates tokens at 25–40 tokens/second on modern multicore CPUs using SIMD vector instructions.
- **Supply-Chain Verification**: During `docker build`, the model file is fetched from Hugging Face Hub and verified against SHA-256 hash `6a1a2eb6d15622bf3c96857206351ba97e1af16c30d7a74ee38970e434e9407e`.

### 6.2 Prompt Engineering & Context Management
Conversations can exceed LLM token limits. The LLM engine implements sliding-window context bounding:
- Context limit: 4,096 tokens.
- Truncation rule: Keeps system instructions and the $N$ most recent messages, gracefully discarding older messages to prevent out-of-memory or context-overflow errors.

#### Smart Replies Prompt Structure
```text
System: You are an AI assistant in a collaboration chat. Suggest 3 short, natural, context-aware reply options.
Context: Conversation in [Channel Name]
Chat History:
Alice: Are we on track for the release today?
Bob: Yes, all tests passed and deployment is ready for 3 PM.
Current message: Great, let me know once it is live.
Instructions: Output exactly 3 suggestions, one per line, starting with 1., 2., 3. Keep each under 10 words.
```

#### Summarization Prompt Structure
```text
System: You are an executive AI assistant. Summarize the following conversation clearly and concisely.
Context: Discussion in [Channel Name]
Chat History:
[Full chronological conversation messages]
Instructions: Produce a concise markdown summary with bullet points under headings:
- Key Topics Discussed
- Decisions Made
- Action Items
```

---

## 7. Web UI & Gateway Architecture

### 7.1 Protocol Translation Gateway
The browser speaks standard JSON REST, while the backend is pure gRPC. `app/web_gateway.py` embeds a lightweight HTTP server (`http.server.HTTPServer`):
- Handles CORS, static asset delivery, and multipart form file parsing.
- Maintains a persistent `grpc.insecure_channel("localhost:50051")`.
- Translates REST endpoints (`/api/signup`, `/api/login`, `/api/messages/dm`, etc.) directly into gRPC requests and serializes protobuf responses back into JSON.

### 7.2 Multi-Tab In-Memory Session Isolation
A critical requirement is the ability to test multi-user real-time interaction on a single computer.
- **Why NOT `localStorage` or `document.cookie`?** Shared browser storage causes every tab in the same browser to share the same login session. Logging into Tab 2 as Bob would overwrite Alice in Tab 1.
- **The Design**: All session tokens and user state are stored in JavaScript runtime memory (`let currentUser = null;`).
- **Benefit**: A developer can open Tab 1 (Alice), Tab 2 (Bob), and Tab 3 (Charlie) in the exact same browser window without needing incognito modes. Each tab operates as an independent client node.

---

## 8. Build-Time Static Self-Test Harness

Reliability in distributed systems requires that faulty builds never reach staging or production. Both `Dockerfile.app` and `Dockerfile.llm` execute static unit tests at Docker build time (`RUN python -m unittest discover -v tests`).

```
Docker Build Phase:
Step 1: Install Python dependencies
Step 2: Compile Protobuf definitions → /generated/
Step 3: RUN python -m unittest discover -v tests
        ├── test_signup_validation_and_duplicate_rejection ..... OK
        ├── test_login_logout_and_active_status ................ OK
        ├── test_direct_messaging_and_history .................. OK
        ├── test_concurrent_duplicate_retries_create_one_message OK
        ├── test_group_creation_and_admin_permissions .......... OK
        ├── test_file_upload_and_download ...................... OK
        ├── test_smart_replies_returns_three_unique ............ OK
        ├── test_context_is_bounded_and_recent_messages_kept ... OK
        └── test_mock_fallback_when_no_model ................... OK
Step 4: Export production container image (Only if 10/10 tests pass)
```

---

## 9. Failure Modes & Operational Resilience

| Failure Scenario | System Behavior | Recovery Mechanism |
|---|---|---|
| **Abrupt Client Disconnect** | Browser tab closed without clicking Logout. | Presence sweeper detects silence after 20 seconds and updates status to `inactive`. |
| **LLM Server Unavailable** | `llm-server` container stopped or restarting. | App node catches gRPC connection error, falls back to deterministic rule-based suggestions, returning HTTP 200 without crashing the chat. |
| **Duplicate Message Submission** | Network lag causes user to double-click "Send". | SQLite detects duplicate `client_request_id` via unique index, replays existing message ID without duplicate creation. |
| **Non-Admin Group Tampering** | Malicious client submits group rename/member removal. | App node checks role in `group_members`; returns `Permission Denied` without mutating state. |
| **Container Crash / Node Restart** | Docker daemon or host reboots. | Persistent Docker volume `node1-data` preserves `/data/chat.db` and `/data/files/`. Containers restart with `restart: unless-stopped`. |

---

## 10. Verification & Quick Reference

### 10.1 Service Endpoint Summary
- **Web UI & REST Gateway**: `http://localhost:8000`
- **App Node gRPC Endpoint**: `localhost:50051`
- **LLM Server gRPC Endpoint**: `localhost:50060`

### 10.2 Management Commands
- **Start cluster in background**:
  ```bash
  docker compose up -d
  ```
- **Inspect live logs**:
  ```bash
  docker compose logs -f app-node-1
  docker compose logs -f llm-server
  ```
- **Run local unit tests outside Docker**:
  ```bash
  PYTHONPATH=.:generated .venv/bin/python -m unittest discover -v tests
  ```
- **Stop cluster and preserve data**:
  ```bash
  docker compose down
  ```
- **Clean slate reset (wipe all volumes and database)**:
  ```bash
  docker compose down -v
  ```
