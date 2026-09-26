# Milestone 1 recording runbook

This runbook starts the standalone M1 application, local LLM service, and
self-checking CLI demo. The demo exits nonzero if a required check fails. It
creates a run-specific channel and temporary account, then removes them through
the application APIs. The persistent Docker volume is retained.

## One-time setup

1. Install and start Docker Desktop, then wait for the Docker engine to be ready.
2. Open PowerShell in the repository root.
3. Build the application and model images while internet access is available:

   ```powershell
   docker compose build
   ```

   The LLM image downloads Qwen2.5-1.5B-Instruct Q4_K_M at a pinned source
   revision, verifies its SHA-256 digest, and stores it inside the local image.
   The model is about 1.12 GB and is not part of the source ZIP.
4. The built images can then be run without internet access. The LLM container
   loads the model before its gRPC port passes the readiness check.

## Recording run

Start screen recording with the terminal visible, then run this exact command
from the repository root:

```powershell
docker compose up --abort-on-container-exit --exit-code-from client-runner
```

The CLI prints a `PASS` line only when all demo checks have succeeded. Stop the
recording after the command exits with code `0`. Any other exit means the demo
did not pass; inspect the preceding service logs and fix the issue before
recording again.

## Suggested 5–10 minute narration order

1. **System status and setup:** point out the standalone app node and separate
   LLM gRPC service. State that Raft replication and fault recovery are M2.
2. **Authentication and roles:** show admin, Alice, and Bob logins and the
   channel list. Show Alice's attempt to create a channel returning
   `PERMISSION_DENIED`.
3. **Channels and chat:** show the admin-created unique channel, Alice and Bob
   joining, and their timestamped messages.
4. **Reliability:** explain that a retried message returns the same message ID.
   Show Alice's live stream receiving Bob's new message, then the persisted
   ordered history.
5. **Presence and file sharing:** show Alice and Bob as online members, then
   upload `demo-notes.txt` and download it with a byte-for-byte comparison.
6. **Local AI:** show three smart replies, a summary, and a concrete next step.
   These use the local Qwen model through client → app gRPC → LLM gRPC.
7. **Administration and cleanup:** show a temporary account being removed and
   denied login, followed by deletion of the run-specific channel and its file
   metadata.
8. **Result:** point to the CLI's final `PASS` message and exit code `0`.

## Repeat runs and storage

The demo uses a fresh random suffix for its channel and temporary user on each
run and cleans them up after success. The normal `node1-data` volume is
preserved. To test with a separate clean database without touching that
existing volume, use a distinct Compose project name:

```powershell
docker compose -p m1-clean up --build --abort-on-container-exit --exit-code-from client-runner
docker compose -p m1-clean up --force-recreate --abort-on-container-exit --exit-code-from client-runner
```

The second command reuses the `m1-clean` volume. Do not add `-v` when stopping
the normal project; that would delete persistent application data.

## If the LLM service is unavailable

The app node does not depend on LLM readiness. Chat, channel, and file RPCs
continue to work; AI RPCs return a bounded error instead of a mock response.
Check `docker compose ps` and `docker compose logs llm-server` before recording.
After building the model image once, starting the demo does not need external
network access.

To verify graceful degradation on a running app, stop the LLM service and run
the smoke check, then start the model service again:

```powershell
docker compose stop llm-server
docker compose run --rm client-runner python scripts/smoke_llm_unavailable.py --server app-node-1:50051
docker compose start llm-server
```

## M1 scope

The app is a single standalone node backed by SQLite. Raft leader election,
replicated writes, multi-node consistency, and fault recovery are explicitly
outside this M1 demo and remain M2 work.

## Verification snapshot (September 26, 2026)

- `docker compose build` completed; the model SHA-256 matched the pinned digest.
- The app-image unittest suite passed all 12 tests, including persistent startup
  seeding, SQLite rollback, concurrent idempotent sends, equal-timestamp history,
  multi-session presence, expiry, account removal, and file cleanup.
- The complete real-gRPC CLI demo passed on a separate clean Compose volume and
  on the existing project volume, including repeated restarts and the runbook's
  exact start command. The existing volume was preserved; no volume deletion
  was performed.
- The streaming demo confirmed that leaving a channel revokes an already-open
  stream, and non-members could not access history, presence, or files.
- The LLM service became healthy after loading the model. Health checks use only
  gRPC port readiness and generated no inference RPCs.
- On this machine, observed inference times ranged from 4.3–8.8 seconds for
  smart replies, 3.1–5.5 seconds for summaries, and 1.6–3.9 seconds for
  next-step suggestions. Docker samples showed about 814 MiB while idle after
  loading and 856 MiB during inference.
- With the LLM service stopped, chat send/history still succeeded and the AI RPC
  returned `LLM server unavailable`.
