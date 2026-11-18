# Distributed Real-Time Chat & Collaboration

A lightweight, real-time chat platform powered by **gRPC microservices**, **Raft consensus**, and a **local LLM** for intelligent summaries and smart replies.

---

## 1. Quick Start

### Local URLs

After starting the stack, open these addresses on the machine running Docker:

| Service | Default address |
|---|---|
| Chat app | [http://localhost:8000](http://localhost:8000) |
| Raft Observatory dashboard | [http://localhost:8088](http://localhost:8088) |

### Start & Stop Scripts

| OS | Start | Stop |
|---|---|---|
| **Windows (CMD)** | `.\scripts\start.bat` | `.\scripts\stop.bat` |
| **Windows (PowerShell)** | `.\scripts\start.ps1` | `.\scripts\stop.ps1` |
| **macOS / Linux** | `bash scripts/start.sh` | `bash scripts/stop.sh` |

- **Logs**: Streamed live in the console and written to `logs/app.log`.
- **Teardown**: Press `Ctrl+C` or run the stop script to terminate containers and networks cleanly.

### Cluster Sizing & Scaling

Docker Compose starts 6 app replicas behind Nginx by default. The cluster requires a strict majority (`(N/2) + 1` = 4 votes) to commit writes or elect a leader, tolerating up to 2 failed nodes.

Scale replicas dynamically during runtime:

```bash
docker compose up -d --no-deps --scale app-node=7 app-node
docker compose up -d --no-deps --scale app-node=6 app-node
```

Key environment variables:
- `RAFT_VOTER_COUNT`: Number of initial voting nodes (default: `6`).
- `APP_REPLICA_SLOTS`: Pre-allocated upstream slots in Nginx for dynamic discovery (default: `16`).

Run automated failover verification:
```bash
python scripts/verify_raft.py --failover-test
```

### Raft Observatory

Open [http://localhost:8088](http://localhost:8088) to monitor the cluster in real time. To run without the local LLM container:

```bash
docker compose up -d --build app-node nginx-lb dashboard
```

Dashboard capabilities:
- **Trace Chat Writes**: Follows requests from client submission through leader append, quorum ACK, and state machine commit.
- **Failover Testing**: Stop the active leader to observe Pre-Vote and election in real time.
- **Runtime Scaling**: Add or remove replicas and observe live log synchronization.

---

## 2. Core Features

- **Direct & Group Chats**: Real-time 1-on-1 direct messaging and multi-user group channels.
- **Admin Group Controls**: Group creators can add/remove members, rename channels, or promote other members to admins.
- **File Sharing**: Inline image previews and PDF file downloads with server-side authorization.
- **Presence Indicators**: Live active/inactive status and last-seen timestamps.
- **Local AI Assistance**:
  - **Smart Replies**: Contextual, one-click reply suggestions for the active conversation.
  - **Chat Summaries**: Concise, grounded summaries personalized to the user without external cloud exposure.
- **Real-Time Sync**: Server-Sent Events (SSE) push updates instantly without polling.

---

## 3. Architecture

- **App replicas (`app-node`)**: Replicas behind Nginx with session affinity. Each maintains an independent SQLite database and file storage, synchronizing writes through Raft.
- **Raft Consensus**: Leader election with Pre-Vote disruption protection, monotonic log replication, and deterministic state machines.
- **LLM Node (`llm-server`)**: Dedicated container on port `50060` running a quantized local model (Qwen 2.5) with GPU acceleration and CPU fallback.
- **Observability Dashboard (`dashboard`)**: Visual telemetry monitoring cluster roles, terms, commit indexes, and active user socket pinning.
- **Inter-Service Communication**: Pure gRPC with Protobuf definitions (`chat.proto`, `raft.proto`, `llm.proto`).

---

## 4. Project Layout

```text
├── app/          # Chat service, auth, presence, file manager & HTTP gateway
├── raft/         # Raft consensus engine, WAL log, and state machine
├── dashboard/    # Real-time consensus telemetry server & visual cockpit
├── llm/          # Local inference engine, prompt templates & gRPC server
├── web/          # Single-Page Application (HTML, CSS, Vanilla JS)
├── proto/        # gRPC Protobuf definitions (chat.proto, raft.proto, llm.proto)
├── tests/        # Automated unit and integration test suites
├── docker/       # Dockerfiles for app, dashboard, and LLM services
├── scripts/      # Startup, teardown, and cluster verification scripts
└── docker-compose.yml
```
