# Distributed Real-time Chat and Collaboration Tool

A real-time chat platform developed in milestones. M1 is a standalone gRPC
application with persistent chat features and local LLM assistance; M2 adds
Raft consensus, replicated state, and fault-tolerance work.

> Academic project — Advanced Operating Systems / Distributed Systems  
> Language: Python · Transport: gRPC · Consensus: Raft · Storage: SQLite · Runtime: Docker Compose

---

## Quick Start

```bash
# Clone and enter the repo
git clone <repo-url>
cd distributed-realtime-chat

# Windows PowerShell: prepares missing images, waits for service health, and runs the demo
.\scripts\start-demo.ps1

# macOS/Linux (same actions; use --rebuild after changing source)
bash scripts/start-demo.sh
# bash scripts/start-demo.sh --rebuild

# Or keep the cluster alive and use the interactive CLI
docker compose up -d llm-server app-node-1
python -m pip install grpcio grpcio-tools protobuf
./scripts/generate_proto.ps1             # Windows PowerShell
# bash scripts/generate_proto.sh         # Linux/macOS
python client/client.py --server localhost:50051
```

The startup scripts build images if any are missing. The first build needs
internet access to download the pinned 1.12 GB local model into the `llm-server`
image. Later starts use the built image and model locally; runtime internet
access is not needed. Use `-Rebuild` on Windows or `--rebuild` on macOS/Linux to
rebuild images after changing source. The script waits for both the app and model
service health checks before starting the demo.

### Default credentials

| User | Password | Role |
|------|----------|------|
| admin | admin123 | ADMIN |
| alice | alice123 | USER |
| bob | bob123 | USER |

---

## Milestones

| Milestone | Deadline | Status |
|-----------|----------|--------|
| **M1** — standalone gRPC app + local LLM + chat features | Sep 28, 2026 | Implementation and acceptance demo verified (Sep 26, 2026) |
| **M2** — Raft consensus, replication, fault tolerance | Nov 18, 2026 | Not started; M1 runs in standalone mode |

---

## Features (Milestone 1)

### Authentication
- `Login` / `Logout` with UUID session tokens
- SHA-256 password hashing
- 24-hour session TTL
- Role-based access: `ADMIN` / `USER`

### Channels
- Create / delete / join / leave / list channels
- Admin-only create and delete
- Creator auto-joined on creation

### Messaging
- Send and retrieve messages
- **Idempotent delivery** via `client_request_id` — retried sends never duplicate
- Millisecond-precision timestamps
- Server-side streaming (`StreamMessages`) for real-time push
- Raft log index field ready for M2

### Presence
- Heartbeat-based `ONLINE` / `OFFLINE` tracking
- Background sweep marks idle users offline after 60 s
- Per-channel presence query

### File Sharing
- Upload files (up to 50 MB) associated with a channel
- Download by `file_id`
- List files per channel
- Metadata stored in SQLite; binary stored on local filesystem

### Admin
- Add / remove users
- Create / delete channels

### LLM Integration (separate server)
- **Smart Replies** — 3 context-aware reply suggestions
- **Conversation Summary** — bullet-point summary of recent messages
- **Context Suggestion** — next-action recommendation based on conversation
- Local Qwen2.5 1.5B Instruct Q4_K_M GGUF model served over the existing LLM gRPC API
- Model is loaded once before the LLM service reports healthy; inference is serialized and time bounded
- If the model service is unavailable, AI responses report an error while chat remains available

---

## Project Structure

```
distributed-realtime-chat/
├── proto/                  # Protobuf definitions
│   ├── chat.proto          # Client ↔ App server RPCs
│   ├── raft.proto          # Raft inter-node RPCs (M2)
│   └── llm.proto           # App server → LLM server RPCs
│
├── app/                    # Application node (monolithic)
│   ├── server.py           # gRPC ChatService impl + seeding
│   ├── auth/manager.py     # Login, logout, token validation
│   ├── chat/manager.py     # Channels + messages + streaming
│   ├── presence/manager.py # Heartbeat presence tracking
│   └── files/manager.py    # File upload/download
│
├── raft/                   # Raft consensus (M2 stubs in M1)
│   ├── node.py             # RaftNode (STANDALONE in M1)
│   ├── log.py              # SQLite-backed RaftLog
│   └── state_machine.py    # StateMachine applying committed entries
│
├── llm/                    # LLM inference server
│   ├── server.py           # gRPC LLMService impl
│   ├── inference.py        # Local llama.cpp inference and response handling
│   └── prompts.py          # Prompt templates
│
├── storage/
│   └── database.py         # Thread-safe SQLite wrapper
│
├── client/
│   └── client.py           # Interactive CLI + --demo mode
│
├── scripts/
│   ├── generate_proto.sh   # Linux/Mac proto compilation
│   └── generate_proto.ps1  # Windows proto compilation
│
├── docker/
│   ├── Dockerfile.app      # App node image
│   └── Dockerfile.llm      # LLM server image
│
├── docker-compose.yml      # M1: 1 node + LLM + client runner
├── requirements.txt        # grpcio, grpcio-tools, protobuf
├── requirements-llm.txt    # CPU llama.cpp wheel + pinned HF downloader
├── tests/                  # M1 manager and inference unit tests
├── THIRD_PARTY_NOTICES.md  # Model and runtime licenses/revisions
└── docs/
    ├── ARCHITECTURE.md
    ├── WALKTHROUGH.md
    ├── MILESTONE1_DEMO.md
    ├── REFLECTION_TEMPLATE.md
    └── MILESTONE2.md
```

---

## Model setup and offline runs

The model version, revision, checksum, and license are recorded in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md). The startup scripts build
missing images automatically; alternatively, to prepare images without starting
the demo, run `docker compose build` while internet access is available. Docker
verifies the model checksum while building the image. The 1.12 GB model is not
stored in the source tree or source ZIP. Once the image exists locally, the demo
can run offline.

If the model image has not been prepared, the LLM service intentionally does not
advertise readiness or return canned replies. Check the build output and LLM
health status before recording the demo.

---

## Running Tests / Demos

```bash
# Unit checks run inside the app image
docker compose run --rm --no-deps client-runner python -m unittest discover -s tests -v

# Automated end-to-end demo startup
.\scripts\start-demo.ps1       # Windows PowerShell
bash scripts/start-demo.sh      # macOS/Linux

# Interactive CLI
python client/client.py --server localhost:50051

# CLI commands
login admin admin123
channels
join <channel_id>
use <channel_id>
send Hello, world!
history 20
presence
smartreply
summarize
suggest
logout
```

See [`docs/MILESTONE1_DEMO.md`](docs/MILESTONE1_DEMO.md) for first-time setup,
repeat runs against the persistent volume, the exact recording order, and the
checks the automated CLI demo performs.

---

## Fault-injection (Milestone 2 demos)

```bash
# Kill the leader
docker stop app-node-1

# Pause a follower (network partition simulation)
docker pause app-node-3

# Resume
docker unpause app-node-3
docker start app-node-1
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `node-1` | Node identifier in logs and Raft |
| `PORT` | `50051` | gRPC listen port |
| `DB_PATH` | `/data/chat.db` | SQLite database path |
| `FILE_STORAGE_PATH` | `/data/files/` | Binary file storage directory |
| `LLM_SERVER` | `llm-server:50060` | LLM server address |
| `LLM_PORT` | `50060` | LLM server listen port |
| `PEERS` | _(empty)_ | Comma-separated peer `host:port` list (M2) |

---

## License

See [LICENSE](LICENSE).
