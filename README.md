# Distributed Real-time Chat and Collaboration Tool

A distributed real-time chat and collaboration platform designed to demonstrate:
- **gRPC-based inter-service communication**
- **Replicated application state and reliable delivery**
- **1-on-1 Direct Messaging & Group Chat with Admin controls**
- **PDF & Image file exchange**
- **Local CPU-optimized LLM assistance (Smart Replies & Summarization over full history)**
- **Modern Responsive Web UI & Build-Time Self-Tests**
- **Docker-only containerized architecture with build-time static verification**

---

## 1. Quick Start

### Run the Application Script (Live Web UI)
The startup script automatically handles Python virtual environment creation/selection, dependencies, performs a clean-slate teardown (`docker compose down -v`), rebuilds images executing static self-tests at build time, starts the application and local LLM services, and keeps the cluster live for interactive browser usage:

**Windows (Command Prompt / PowerShell / File Explorer)**:
```cmd
.\start.bat
```
*(or via PowerShell directly: `powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1`)*

**Linux / macOS (Bash)**:
```bash
bash scripts/start.sh
```

Once running:
- Open **[http://localhost:8000](http://localhost:8000)** in your browser to sign up and chat.
- Live logs stream in the console.
- Press **`Ctrl+C`** to gracefully shut down the cluster and clean up volumes.
---

## 2. Key Requirements & Implementation

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
| **13** | **Proper Database Schema** | Clean SQLite tables: `users`, `sessions`, `groups`, `group_members`, `messages`, `files`. |
| **14** | **gRPC Inter-service Architecture** | gRPC exclusively used for all client-to-app and app-to-LLM communication. |

---

## 3. Project Structure

```
distributed-realtime-chat/
├── proto/                  # Protocol buffer definitions
│   ├── chat.proto          # Client ↔ App Node RPCs
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
├── client/                 # Python CLI Client
│   ├── client.py           # CLI entrypoint & argument parser
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
├── docker-compose.yml      # Cluster deployment (App Node :50051/:8000, LLM Server :50060)
├── start.bat               # Windows one-click startup (venv, deps, docker)
└── scripts/
    ├── start.ps1           # PowerShell orchestrator
    └── start.sh            # Linux/macOS orchestrator
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

## 5. Build-Time Static Self-Tests

Only static tests run at Docker build time to guarantee system correctness without running runtime test scripts:
- **`tests/test_managers.py`**: Validates user signup restrictions, session TTL, DM messaging, group creation with admin controls, file upload/download, and concurrent retry idempotency.
- **`tests/test_llm_inference.py`**: Validates chat context bounding, prompt construction, and graceful fallback.
- Executed during `docker compose build` in both `Dockerfile.app` and `Dockerfile.llm`. If any test fails, the image build aborts immediately.
