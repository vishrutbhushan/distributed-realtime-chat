# Distributed Real-time Chat and Collaboration Tool

A distributed real-time chat and collaboration platform designed to demonstrate:
- **gRPC-based inter-service communication**
- **Replicated application state and reliable delivery**
- **1-on-1 Direct Messaging & Group Chat with Admin controls**
- **PDF & Image file exchange**
- **Local CPU-optimized LLM assistance (Smart Replies & Summarization over full history)**
- **Web UI & Automated Verification Suite**
- **Architectural readiness for Raft consensus (Milestone 2)**

---

## 1. Quick Start (Docker Only)

### Build and Run Full Automated Verification Suite
```bash
docker compose build --no-cache
docker compose up --abort-on-container-exit
```
This builds all images, starts `llm-server`, `app-node-1`, and runs `client-runner` which executes the 14-point Milestone 1 verification suite, exits with code 0, and stops the containers.

### Run Web UI Cluster in Detached Mode
```bash
docker compose up -d llm-server app-node-1
```
Open **[http://localhost:8000](http://localhost:8000)** in any browser.

---

## 2. Milestone 1 Key Requirements & Implementation

| # | Requirement | Implementation Details |
|---|-------------|------------------------|
| **1** | **Zero default channels & users** | Database starts completely empty (`/data/chat.db`). Users must sign up. |
| **2** | **Web UI for all users** | Embedded HTTP Gateway on port 8000 serving `web/index.html` and translating REST calls into gRPC. |
| **3** | **Signup & Login restrictions** | Username: 3–20 alphanumeric chars/underscores (`^[a-zA-Z0-9_]{3,20}$`). Password: minimum 4 chars. |
| **4** | **Active / Inactive Status** | Status is set to `active` upon signup/login, and `inactive` upon logout. Tracked in `users` table and refreshed via heartbeat. |
| **5** | **1-on-1 Direct Messaging** | `SendDirectMessage` and `GetDirectMessages` RPCs. Messages indexed by `(sender_id, recipient_id, timestamp)`. Idempotency via `client_request_id`. |
| **6** | **PDF & Image File Sharing** | `UploadFile` and `DownloadFile` RPCs. Binary stored in `/data/files/`, metadata in SQLite `files` table. Inline rendering for images, downloadable card for PDFs. |
| **7** | **Group Creation with User Checklist** | `CreateGroup` accepts a name and checklist of initial member user IDs during creation. Creator is `ADMIN` by default; members immediately see the group. |
| **8** | **Admin-only Group Controls** | `UpdateGroup` enforces that only group `ADMIN` can rename group, add members, remove members, or promote members to admin. Non-admin attempts return `Permission Denied`. |
| **9** | **Standalone LLM Instance** | Dedicated container (`llm-server`) listening on gRPC port `50060`. CPU-optimized mock active; Llama.cpp, Transformers, and Ollama backends ready to uncomment. |
| **10** | **LLM Features** | a) Smart Replies (generates 3 concise suggestions). b) Summarize (generates bullet-point digest). |
| **11** | **Pass Full Chat History** | App node passes the entire conversation history to the LLM server for context-aware generation. |
| **12** | **UI Options** | Web UI provides dedicated buttons for "Smart Replies" (clickable pills) and "Summarize" (modal summary). |
| **13** | **Proper Database Schema** | Clean SQLite tables: `users`, `sessions`, `groups`, `group_members`, `messages`, `files`, `raft_state`, `raft_log`. |
| **14** | **gRPC Inter-service & M2 Readiness** | gRPC exclusively used for all client-to-app and app-to-LLM communication. Monolithic node design structured for Raft log replication in M2. |

---

## 3. Project Structure

```
distributed-realtime-chat/
├── proto/                  # Protocol buffer definitions
│   ├── chat.proto          # Client ↔ App Node RPCs
│   ├── raft.proto          # Inter-node Raft consensus RPCs (Milestone 2)
│   └── llm.proto           # App Node ↔ LLM Server RPCs
│
├── app/                    # Monolithic Application Node
│   ├── server.py           # gRPC ChatService + HTTP Web Gateway (:8000)
│   ├── auth/manager.py     # Signup, login, logout, token session, active/inactive
│   ├── chat/manager.py     # DMs, groups, memberships, real-time queues
│   ├── presence/manager.py # Background idle sweeper
│   └── files/manager.py    # Binary file store + SQLite metadata
│
├── web/                    # Single-Page Web Application
│   └── index.html          # Modern UI (Auth, DMs, Groups, Files, AI toolbar)
│
├── llm/                    # Standalone LLM Server
│   ├── server.py           # gRPC LLMService implementation (:50060)
│   ├── inference.py        # CPU inference engine (mock + real model backends)
│   └── prompts.py          # Prompt formatting templates
│
├── storage/                # Persistence Layer
│   └── database.py         # Thread-safe SQLite with WAL & concurrency lock
│
├── raft/                   # Consensus Engine (Milestone 2 stubs)
│   ├── node.py             # RaftNode (STANDALONE in M1, consensus in M2)
│   ├── log.py              # Persistent RaftLog backed by SQLite
│   └── state_machine.py    # StateMachine for committed log entries
│
├── client/                 # Python Client
│   └── client.py           # Interactive CLI + 14-point automated verification
│
├── docker/
│   ├── Dockerfile.app      # App Node image (includes Web UI and proto compile)
│   └── Dockerfile.llm      # LLM Server image
│
├── docker-compose.yml      # Cluster deployment (App, LLM, Client Runner)
└── docs/
    ├── ARCHITECTURE.md     # In-depth architectural blueprint
    ├── WALKTHROUGH.md      # Detailed verification run walkthrough
    └── MILESTONE2.md       # Roadmap and pending changes for Milestone 2
```

---

## 4. Web UI Features (`http://localhost:8000`)

1. **Authentication**: Switch between Log In and Sign Up tabs.
2. **Directory**: Real-time listing of all other users with active (green) and inactive (gray) indicators and last-seen timestamps.
3. **1-on-1 Direct Messaging**: Click any user to start a DM session.
4. **Group Collaboration**:
   - Create new groups with custom name and user checklist.
   - Creator is `ADMIN` by default; members immediately see the group.
   - Admin settings modal: Rename group, add member, remove member, make another user admin.
5. **File Exchange**:
   - Attach PDF or Image files using the paperclip button.
   - Images are rendered directly inside chat bubbles.
   - PDFs display as file cards with instant download buttons.
6. **AI Tools**:
   - **Smart Replies**: Suggests 3 one-click replies based on the entire chat history.
   - **Summarize**: Generates an intelligent summary modal of the whole discussion.

---

## 5. Enabling a Real LLM (Milestone 2)

By default, `USE_MOCK = True` is enabled in `llm/inference.py` so the system runs immediately on any CPU without downloading gigabytes of weights.

To enable a real local CPU model:
1. Open [`llm/inference.py`](llm/inference.py) and set `USE_MOCK = False`.
2. Uncomment your preferred backend (Backend A: `llama.cpp` for GGUF, Backend B: `transformers` for HF, or Backend C: `ollama`).
3. Uncomment the dependencies in [`docker/Dockerfile.llm`](docker/Dockerfile.llm) and rebuild:
   ```bash
   docker compose build llm-server && docker compose up -d llm-server
   ```
