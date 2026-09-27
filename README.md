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

- **App Node (`app-node-1`)**: Handles authentication, chat logic, presence, and file storage via thread-safe SQLite (`WAL` mode). Includes an HTTP gateway on port `8000` that translates browser requests into internal gRPC calls (`50051`).
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
