# Distributed Real-Time Chat & Collaboration

A lightweight,  real-time chat platform powered by **gRPC microservices** and a **local LLM** for intelligent summaries and smart replies.

---

## 1. Quick Start

Run the appropriate script to start or stop the full stack:

| OS | Start | Stop |
|---|---|---|
| **Windows (CMD)** | `.\scripts\start.bat` | `.\scripts\stop.bat` |
| **Windows (PowerShell)** | `.\scripts\start.ps1` | `.\scripts\stop.ps1` |
| **macOS / Linux** | `bash scripts/start.sh` | `bash scripts/stop.sh` |

- **Web App**: Open [http://localhost:8000](http://localhost:8000) to sign up and start chatting.
- **Logs**: Streamed live in your console and saved to `logs/app.log`.
- **Teardown**: Press `Ctrl+C` or run the stop script to cleanly remove all containers and volumes with zero lingering background processes.

Docker Compose starts six app replicas behind Nginx. All six can vote and run
for leader. Elections and commits need four votes, so the default cluster
can lose up to two voters. Extra replicas can join or leave while chat is running:

```powershell
docker compose up -d --no-deps --scale app-node=7 app-node
docker compose up -d --no-deps --scale app-node=6 app-node
```

Keep the replica count at or above the configured voter count. To demo a
voter failure and check that the old leader catches up after it restarts, run
`python scripts/verify_raft.py --failover-test`. Set
`RAFT_VOTER_COUNT` when provisioning a fresh cluster. Live scaling changes
the number of replicas, while the configured voting set stays fixed.
When upgrading an existing three-voter demo, first verify that all six replicas
have applied the same commit index. Shut down all app replicas before starting
with the new voting set. Keep the data volume and avoid a rolling restart across
different voting configurations.
Nginx assigns an opaque session cookie and resolves replica addresses as they
join or leave. It prepares 16 replica slots by default. Set
`APP_REPLICA_SLOTS` before starting Nginx if you need more.

### Raft Observatory

Open [http://localhost:8088](http://localhost:8088) after startup. To run just
the chat infrastructure and dashboard without building the LLM image:

```powershell
docker compose up -d --build app-node nginx-lb dashboard
```

- **Send a chat message** creates demo accounts on first use and sends a real
  direct message through the chat API. The dashboard follows its request ID
  through receipt, replication, commit and application. Expand the technical
  sequence to inspect the RPCs or replay them one event at a time.
- **Stop current leader** stops the real leader container. Watch the votes
  and election, then send another message. Select the stopped node and click
  **Restart** to see it return and catch up.
- **Add a replica** starts another app container in the same Compose project.
  It catches up and serves traffic without changing the voter count. Use a
  node card or the replica selector to stop or restart any node. Stopping
  enough voters to lose a majority prevents new commits until a voter returns.

The dashboard port binds to loopback. The dashboard uses the Docker daemon
socket for container controls, so run it only on a trusted demo machine. Its
events are limited to Raft metadata and live in bounded memory buffers.

---

## 2. Core Features

- **Direct & Group Chats**: Real-time 1-on-1 direct messaging and multi-user group chats.
- **Admin Group Controls**: Group creators are admins who can add/remove members, rename the group, or promote others.
- **File Sharing**: Send images (rendered directly in chat) and PDFs (available with one-click download).
- **Presence Indicators**: Live active/inactive status and last-seen timestamps for all users.
- **Local AI Assistance**:
  - **Smart Replies**: Contextual, one-click reply suggestions for the active conversation.
  - **Chat Summaries**: Informative, grounded summaries personalized to the user ("You") without data leaving your machine.
- **Real-Time Sync**: Efficient Server-Sent Events (SSE) push updates instantly without idle polling.

---

## 3. Architecture

- **App replicas (`app-node`)**: Nginx provides one browser URL. Each replica has its own SQLite database and file directory. Replicas use Raft for writes and SSE for browser updates.
- **LLM Node (`llm-server`)**: Dedicated container on port `50060` running a quantized local model (Qwen 2.5) with automatic GPU / Metal acceleration and CPU fallback.
- **Inter-Service Communication**: Pure gRPC with Protobuf definitions (`chat.proto` and `llm.proto`).

---

## 4. Project Layout

```text
├── app/          # Core chat service, auth, presence, file manager & HTTP gateway
├── llm/          # Local inference engine, prompt templates & gRPC server
├── web/          # Responsive Single-Page Application (HTML / Vanilla CSS / JS)
├── proto/        # gRPC Protobuf definitions (chat.proto, llm.proto)
├── tests/        # Unit & static integration test suites
├── docker/       # Dockerfiles for app and LLM services
├── scripts/      # Cross-platform start and stop scripts
└── docker-compose.yml
