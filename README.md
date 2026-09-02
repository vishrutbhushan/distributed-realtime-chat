# Distributed Real-time Chat and Collaboration Tool

A distributed real-time chat platform built to demonstrate core distributed-systems concepts:
**gRPC-based communication · Raft consensus · replicated state · message ordering · fault tolerance · LLM integration**

> Academic project — Advanced Operating Systems / Distributed Systems  
> Language: Python · Transport: gRPC · Consensus: Raft · Storage: SQLite · Runtime: Docker Compose

---

## Quick Start

```bash
# Clone and enter the repo
git clone <repo-url>
cd distributed-realtime-chat

# Build all images (compiles proto stubs inside Docker)
docker compose build

# Run the full demo (client exits when done, then cluster stops)
docker compose up --abort-on-container-exit

# Or keep the cluster alive and use the interactive CLI
docker compose up -d llm-server app-node-1
python -m pip install grpcio grpcio-tools protobuf
python scripts/generate_proto.ps1        # Windows
# bash scripts/generate_proto.sh         # Linux/Mac
python client/client.py --server localhost:50051
```

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
| **M1** — gRPC app server + LLM server + full chat features | Sep 28, 2026 | ✅ Complete |
| **M2** — Raft consensus, replication, fault tolerance | Nov 18, 2026 | 🔲 In progress |

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
- Model is mocked by default; three real backends ready to uncomment

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
│   ├── inference.py        # Mock + 3 real backends (commented)
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
├── requirements-llm.txt    # LLM deps (all commented, pick one)
└── docs/
    ├── ARCHITECTURE.md
    ├── WALKTHROUGH.md
    └── MILESTONE2.md
```

---

## Enabling a Real LLM

Edit [`llm/inference.py`](llm/inference.py) and uncomment **one** backend:

| Backend | Model format | Best for |
|---------|-------------|----------|
| **llama.cpp** | GGUF | CPU-optimised, recommended |
| **HuggingFace Transformers** | PyTorch | Any HF model |
| **Ollama REST** | Any Ollama model | Simplest setup |

Then:
1. Set `USE_MOCK = False` in `inference.py`
2. Uncomment the matching block in `requirements-llm.txt`
3. Uncomment the install line in `docker/Dockerfile.llm`
4. `docker compose build llm-server && docker compose up`

---

## Running Tests / Demos

```bash
# Automated end-to-end demo
docker compose up --abort-on-container-exit

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
