# Milestone 1 Demo and Runbook

This guide covers running and verifying Milestone 1:
- The standalone gRPC application node with embedded Web Gateway (`:8000`)
- The dedicated local CPU-optimized LLM server (`:50060`)
- The automated verification suite and interactive Browser Web UI

---

## 1. Quick Startup Options

### Option A: Using the One-Click Startup Script

On Windows (Command Prompt / PowerShell / File Explorer):
```cmd
.\start-demo.bat
```

On Linux / macOS:
```bash
bash scripts/start-demo.sh
```

These scripts tear down previous state, build images (executing static self-tests at build time), start `llm-server` and `app-node-1`, and stream live logs.

### Option B: Using Docker Compose Directly

```bash
# Build images (executes static self-tests)
docker compose build

# Start services in foreground with live logs
docker compose up
```

---

## 2. Accessing the Web UI

Once `app-node-1` is running, open your web browser to:
```
http://localhost:8000
```

1. **Sign Up**: Register two or more test accounts (e.g. `alice`, `bob`).
2. **Directory & Presence**: Observe active status indicators in real time.
3. **Direct Messaging (DMs)**: Chat 1-on-1 between accounts with instantaneous updates.
4. **Group Chat**: Click **+ New Group**, enter a group name, select members from the checklist, and create the group.
5. **Admin Controls**: As the group creator (Admin), rename the group, add new members, remove members, or transfer Admin privileges.
6. **File Sharing**: Upload and preview `.png` / `.jpg` images inline or exchange `.pdf` documents with direct download links.
7. **AI Features**: Click **✨ Smart Reply** to receive 3 context-aware suggestions, or **📝 Summarize** to generate a bulleted summary of the full conversation.

---

## 3. Verifying Fault Tolerance (LLM Graceful Degradation)

To verify that core chat continues operating without interruption if the LLM service goes offline:

1. In another terminal, stop the LLM container:
   ```bash
   docker compose stop llm-server
   ```
2. In the Web UI, continue sending messages, creating groups, and exchanging files — all normal chat functions continue without disruption.
3. Click **✨ Smart Reply** or **📝 Summarize** in the Web UI: the UI shows a clean, bounded notice that AI assistance is temporarily unavailable, without crashing or freezing.
4. Restart the LLM container:
   ```bash
   docker compose start llm-server
   ```
   AI features immediately resume functioning.

---

## 4. Suggested Narration Order for Demo Video

1. **System Overview**: Point out the standalone application server (gRPC `:50051` + Web Gateway `:8000`) and separate LLM server (`:50060`). Mention that multi-node Raft replication is Milestone 2.
2. **Clean Start & Authentication**: Show user signup and login with input validation (minimum 4 characters, valid username syntax).
3. **Presence**: Show user status dynamically toggling between active (online) and inactive.
4. **Direct Messaging & Reliability**: Show 1-on-1 messaging and explain that each message send is idempotent via client UUIDs.
5. **Group Collaboration & Admin Controls**: Create a group with selected members; show admin-only actions (rename, member management).
6. **Media Exchange**: Upload an image and show inline rendering; upload a PDF and download it.
7. **Local LLM Assistance**: Trigger Smart Replies and Summarize, showing that the full conversation history is supplied for context.
8. **Decoupled Architecture**: Stop `llm-server`, show that sending and receiving messages still works, and observe the handled AI error.
