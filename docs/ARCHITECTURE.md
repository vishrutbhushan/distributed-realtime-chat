# Architecture — Distributed Real-time Chat and Collaboration Tool

## 1. System Overview

The system is designed as a **cluster of monolithic application nodes** coordinated via **gRPC** and a **standalone CPU-optimized LLM server**.

```text
                                 ┌──────────────────────────┐
                                 │       LLM SERVER         │
                                 │  (Standalone Instance)   │
                                 │  llm/server.py           │
                                 │  port 50060              │
                                 │                          │
                                 │  • GetSmartReplies       │
                                 │  • SummarizeConversation │
                                 │  • GetContextSuggestion  │
                                 └──────────┬───────────────┘
                                            │ gRPC (llm.proto)
                                            │
         ┌──────────────────────────────────┼──────────────────────────────────┐
         │                                  │                                  │
   ┌─────▼──────┐                    ┌──────▼─────┐                     ┌──────▼──────┐
   │  Node 1    │◄══════════════════►│  Node 2    │◄═══════════════════►│  Node 3     │
   │  STANDALONE│   Raft Consensus   │  FOLLOWER  │    Raft Consensus   │  FOLLOWER   │
   │  (M1 Mode) │    (Milestone 2)   │(Milestone 2│     (Milestone 2)   │(Milestone 2)│
   │            │                    │            │                     │             │
   │ Web Gateway│                    │ Web Gateway│                     │ Web Gateway │
   │ (:8000)    │                    │            │                     │             │
   │ Auth       │                    │ Auth       │                     │ Auth        │
   │ Chat (DMs) │                    │ Chat (DMs) │                     │ Chat (DMs)  │
   │ Groups     │                    │ Groups     │                     │ Groups      │
   │ Presence   │                    │ Presence   │                     │ Presence    │
   │ Files Store│                    │ Files Store│                     │ Files Store │
   │ Raft Node  │                    │ Raft Node  │                     │ Raft Node   │
   │ SQLite DB  │                    │ SQLite DB  │                     │ SQLite DB   │
   │ gRPC :50051│                    │ gRPC :50052│                     │ gRPC :50053 │
   └─────▲──────┘                    └──────▲─────┘                     └──────▲──────┘
         │                                  │                                  │
         └─────────────────┬────────────────┴──────────────────┬───────────────┘
                           │                                   │
              HTTP / REST  │  gRPC (chat.proto)   HTTP / REST  │  gRPC (chat.proto)
                           │                                   │
                    ┌──────▼──────┐                     ┌──────▼──────┐
                    │  Web Users  │                     │ CLI Clients │
                    │ (Browser UI)│                     │  (Client)   │
                    └─────────────┘                     └─────────────┘
```

---

## 2. Design Principles

### 2.1 Replicated Monoliths
Instead of decomposing chat, auth, and presence into fragmented microservices, each application node is a **monolith**:
- Contains the full application stack (Auth, Chat, Groups, Presence, Files, SQLite, Raft).
- Maintains its own local SQLite storage.
- Operates independently and communicates with other nodes strictly via gRPC.
- In Milestone 1, each node operates in `STANDALONE` mode.
- In Milestone 2, state changes (messages, groups, user changes) will be replicated across nodes using Raft consensus before being committed to SQLite.

### 2.2 Dedicated AI / LLM Instance
The LLM inference engine runs on a completely separate server (`llm-server` on port 50060).
- Application nodes never execute heavy inference locally; they invoke `LLMService` via gRPC.
- The entire chat history is passed in the RPC payload, giving the model full conversational context.

### 2.3 Dual Interfaces: Browser Web UI & gRPC
- All inter-service communication and core APIs strictly adhere to **gRPC**.
- To satisfy the browser requirement ("all users will hit the same URL and get a UI"), Node 1 embeds an HTTP Web Gateway on port 8000 that translates browser REST/JSON calls directly to gRPC stubs.

---

## 3. Database Schema (`storage/database.py`)

Each application node manages an isolated SQLite database using WAL mode and `PRAGMA busy_timeout=30000` under process-wide read/write locks:

```sql
-- Registered users (clean start: 0 default users)
CREATE TABLE users (
    user_id       TEXT PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'inactive',  -- 'active' | 'inactive'
    last_seen     INTEGER NOT NULL DEFAULT 0,
    created_at    INTEGER NOT NULL
);

-- Active session tokens (UUID tokens with 7-day TTL)
CREATE TABLE sessions (
    token      TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- Collaboration groups (clean start: 0 default groups)
CREATE TABLE groups (
    group_id   TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (created_by) REFERENCES users(user_id)
);

-- Group memberships with role controls
CREATE TABLE group_members (
    group_id  TEXT NOT NULL,
    user_id   TEXT NOT NULL,
    role      TEXT NOT NULL DEFAULT 'MEMBER',  -- 'ADMIN' | 'MEMBER'
    joined_at INTEGER NOT NULL,
    PRIMARY KEY (group_id, user_id),
    FOREIGN KEY (group_id) REFERENCES groups(group_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id)  REFERENCES users(user_id)   ON DELETE CASCADE
);

-- Replicated message ledger (Direct Messages & Group Messages)
CREATE TABLE messages (
    message_id        TEXT PRIMARY KEY,
    chat_type         TEXT NOT NULL,               -- 'DM' | 'GROUP'
    sender_id         TEXT NOT NULL,
    recipient_id      TEXT DEFAULT NULL,          -- user_id for DM
    group_id          TEXT DEFAULT NULL,          -- group_id for GROUP
    content           TEXT NOT NULL,
    file_id           TEXT DEFAULT NULL,
    timestamp         INTEGER NOT NULL,           -- ms epoch
    client_request_id TEXT UNIQUE,                -- Idempotency key
    raft_log_index    INTEGER DEFAULT 0,          -- Milestone 2 replication index
    FOREIGN KEY (sender_id)    REFERENCES users(user_id),
    FOREIGN KEY (recipient_id) REFERENCES users(user_id),
    FOREIGN KEY (group_id)     REFERENCES groups(group_id) ON DELETE CASCADE
);

-- File metadata (binary stored in /data/files/<file_id>)
CREATE TABLE files (
    file_id          TEXT PRIMARY KEY,
    filename         TEXT NOT NULL,
    file_type        TEXT NOT NULL,               -- 'image' | 'pdf' | 'other'
    content_type     TEXT NOT NULL DEFAULT 'application/octet-stream',
    size_bytes       INTEGER NOT NULL,
    owner_id         TEXT NOT NULL,
    chat_type        TEXT NOT NULL,               -- 'DM' | 'GROUP'
    target_id        TEXT NOT NULL,               -- recipient_id or group_id
    storage_location TEXT NOT NULL,
    uploaded_at      INTEGER NOT NULL,
    FOREIGN KEY (owner_id) REFERENCES users(user_id)
);

-- Raft state & log (Milestone 2 readiness)
CREATE TABLE raft_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE raft_log (
    log_index    INTEGER PRIMARY KEY,
    term         INTEGER NOT NULL,
    command_type TEXT NOT NULL,
    payload      TEXT NOT NULL,
    request_id   TEXT
);
```

---

## 4. Message & Data Flow

### 4.1 Direct Messaging (1-to-1)
1. **Client** calls `SendDirectMessage(token, recipient_user_id, content, client_request_id, file_id)`.
2. **Server** validates `token` via `AuthManager.validate_token()`.
3. Server checks `client_request_id` in `messages` table for idempotency.
4. If fresh, inserts row into `messages` with `chat_type='DM'`.
5. Server pushes new message into real-time streaming queues for both sender and recipient.

### 4.2 Group Messaging & Admin Controls
1. Any user can call `CreateGroup(token, name, initial_member_user_ids)`.
   - Creator is assigned `role='ADMIN'`.
   - Selected initial members are added as `role='MEMBER'`.
   - All members immediately query the group in `ListGroups`.
2. Calling `UpdateGroup(token, group_id, action, target_user_id, new_name)` verifies that the requesting user has `role='ADMIN'`.
   - Non-admins receive `PERMISSION_DENIED`.
   - Admins can `RENAME`, `ADD_MEMBER`, `REMOVE_MEMBER`, and `MAKE_ADMIN`.

### 4.3 LLM Assistance (Smart Replies & Summarization)
1. Client requests smart replies or summary via `GetSmartReplies` / `SummarizeChat`.
2. App node reads all historical messages for the active conversation.
3. App node constructs formatted list of messages (`"username: message"`).
4. App node proxies the call to `LLMService` on port `50060` via gRPC.
5. `LLMService` processes the entire history and returns 3 concise smart replies or a bulleted summary.
