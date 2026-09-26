# Distributed Real-time Chat and Collaboration Tool

A distributed real-time chat and collaboration platform designed to demonstrate:
- **gRPC-based inter-service communication**
- **Replicated application state and reliable delivery**
- **1-on-1 Direct Messaging & Group Chat with Admin controls**
- **PDF & Image file exchange**
- **Local CPU-optimized LLM assistance (Smart Replies & Summarization over full history)**
- **Modern Responsive Web UI & Automated Verification Suite**
- **Docker-only containerized architecture with build-time static verification**
- **Architectural readiness for Raft consensus (Milestone 2)**

---

## 1. Quick Start (Docker Only)

> **Important**: Docker is the sole supported and verified runtime environment.

### Run the One-Click Automated Demo & Verification
The startup script performs a clean-slate teardown (`docker compose down -v`), rebuilds images executing static unit tests at build time, starts services, and runs the 14-point verification suite:

**Windows (Command Prompt / PowerShell / File Explorer)**:
```cmd
.\start-demo.bat
```
*(or via PowerShell directly: `powershell -ExecutionPolicy Bypass -File .\scripts\start-demo.ps1`)*

**Linux / macOS (Bash)**:
```bash
bash scripts/start-demo.sh
```

### Manual Docker Compose Workflow
```bash
# 1. Clean previous state
docker compose down -v --remove-orphans

# 2. Build images (runs unit test suite inside Docker)
docker compose build

# 3. Run the automated 14-point verification test
docker compose up --abort-on-container-exit

# Or run the cluster in the background to use the Web UI
docker compose up -d llm-server app-node-1
```

Open **[http://localhost:8000](http://localhost:8000)** in your browser.

---

## 2. Milestone 1 Key Requirements & Implementation

| # | Requirement | Implementation Details |
|---|-------------|------------------------|
| **1** | **Zero default channels & users** | Database starts completely empty (`/data/chat.db`). Users must sign up. |
| **2** | **Web UI for all users** | Embedded HTTP Gateway on port 8000 serving clean modular HTML/CSS/JS and translating REST calls into gRPC. |
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
│   ├── server.py           # Application entrypoint & runtime orchestrator
│   ├── grpc_server.py      # Core gRPC ChatServicer implementation
│   ├── web_gateway.py      # HTTP REST Web Gateway translating HTTP to gRPC
│   ├── auth/manager.py     # Signup, login, logout, token session, active/inactive
│   ├── chat/manager.py     # DMs, groups, memberships, real-time queues
│   ├── presence/manager.py # Background idle sweeper
│   └── files/manager.py    # Binary file store + SQLite metadata
│
├── web/                    # Modular Single-Page Web Application
│   ├── index.html          # Semantic HTML structure
│   ├── css/
│   │   └── style.css       # Clean stylesheet (layout, modals, typography)
│   └── js/
│       ├── api.js          # REST client communicating with Web Gateway
│       └── app.js          # State management, DOM events, and UI rendering
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
│   ├── client.py           # CLI entrypoint & argument parser
│   ├── demo.py             # 14-point automated verification suite
│   └── repl.py             # Interactive terminal chat REPL
│
├── tests/                  # Static Unit Tests (run at docker build time)
│   ├── test_managers.py    # Tests for auth, chat, idempotency, groups, files
│   └── test_llm_inference.py # Tests for prompt construction & context bounding
│
├── docker/
│   ├── Dockerfile.app      # App Node image (build-time static tests + proto compilation)
│   └── Dockerfile.llm      # LLM Server image (build-time static tests + proto compilation)
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

## 5. Testing and Verification Strategy

- **Build-time Static Tests**: During `docker compose build`, unit tests (`tests/test_managers.py` and `tests/test_llm_inference.py`) run inside the Docker container build steps. If any test fails, the Docker image build aborts immediately.
- **Runtime Integration Tests**: `docker compose up --abort-on-container-exit` executes `client/demo.py`, verifying all 14 end-to-end distributed chat behaviors over actual gRPC network calls.
