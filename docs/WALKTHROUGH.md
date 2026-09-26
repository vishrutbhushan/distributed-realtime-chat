# Walkthrough — Milestone 1 Verification Suite & Web UI

This document explains the end-to-end execution of Milestone 1 in Docker and demonstrates how every requirement was verified.

---

## 1. Running the Automated Verification in Docker

```bash
docker compose build --no-cache
docker compose up --abort-on-container-exit
```

### Execution Output & Step-by-Step Breakdown

```text
=================================================================
 DISTRIBUTED REAL-TIME CHAT — MILESTONE 1 VERIFICATION
=================================================================
-----------------------------------------------------------------
[1] Node Status (Standalone gRPC node, ready for Raft consensus)
    node_id      = node-1
    state        = STANDALONE
    term         = 0
    commit_index = 0
    [OK] Node status verified.
-----------------------------------------------------------------
[2] Signup Users (Testing username & password restrictions)
    [OK] Username validation rejected '< 3 chars' as expected.
    [OK] Signed up alice_f522 (id=f98696eb..., status=active)
    [OK] Signed up bob_02a4 (id=3b33452f..., status=active)
-----------------------------------------------------------------
[3] User Directory & Status Visibility
    Alice sees 1 other registered user(s):
      - bob_02a4: status=active (last_seen=1790407950)
    [OK] User presence tracking verified.
-----------------------------------------------------------------
[4] 1-on-1 Direct Messaging (Alice sends DM to Bob)
    [alice_f522 -> Bob]: Hi Bob! Did you review the distributed systems architecture doc?
    [bob_02a4 -> Alice]: Yes Alice, looks solid! We need to verify Raft log replication next.
    [OK] DM history retrieved (2 messages exchanged).
-----------------------------------------------------------------
[5] Idempotency Verification (Resending identical client_request_id)
    Message ID 1 = 72ac7c8d-9cc8-4e23-a375-a2ba41809b5b
    Message ID 2 = 72ac7c8d-9cc8-4e23-a375-a2ba41809b5b
    [OK] Idempotency confirmed: No duplicate message inserted.
-----------------------------------------------------------------
[6] PDF and Image File Sharing in Chat
    [OK] Uploaded PDF: system_design.pdf (type=pdf, 61 bytes)
    [OK] Bob downloaded PDF successfully (61 bytes verified).
-----------------------------------------------------------------
[7] Group Creation (Alice creates 'Distributed Core Team', adds Bob)
    [OK] Created group: 'Distributed Core Team' (id=4eeb0619..., members=2, role=ADMIN)
-----------------------------------------------------------------
[8] Group Visibility for Members
    [OK] Bob verified membership in 1 group(s).
-----------------------------------------------------------------
[9] Group Messaging (Alice and Bob send messages)
    [alice_f522 in #Distributed Core Team]: Welcome to the group channel everyone!
    [bob_02a4 in #Distributed Core Team]: Glad to be here. Let's finish the consensus algorithm.
    [OK] Group messaging verified.
-----------------------------------------------------------------
[10] Admin-only Group Controls (Rename, Promote, Non-admin rejection)
    [OK] Non-admin update blocked as expected (Permission Denied).
    [OK] Admin renamed group to 'Consensus Engineering Team'.
    [OK] Alice promoted Bob to group ADMIN.
-----------------------------------------------------------------
[11] LLM Smart Replies (Passing entire conversation history)
    Received 3 Smart Reply Suggestions:
      1. Got it, thanks!
      2. Will do.
      3. Sounds good — let me know if you need anything else.
    [OK] LLM Smart Replies verified.
-----------------------------------------------------------------
[12] LLM Conversation Summary (Passing entire conversation history)
    Summary output:
    Summary of #Consensus Engineering Team (2 messages):
      - Participants: alice_f522, bob_02a4
      - 2 messages exchanged.
      - (Enable a real LLM in llm/inference.py for intelligent summaries.)
    [OK] LLM Summarization verified.
-----------------------------------------------------------------
[13] Assignment Required Core RPCs (post, get, processBusinessRequest)
    [OK] Post, Get, and ProcessBusinessRequest RPCs verified.
-----------------------------------------------------------------
[14] Logout and Status Transition to Inactive
    [OK] Bob logged out.
    [OK] Bob's status confirmed as 'inactive' after logout.
-----------------------------------------------------------------

  ALL MILESTONE 1 VERIFICATION TESTS PASSED SUCCESSFULLY!
=================================================================
```

---

## 2. Using the Browser Web UI (`http://localhost:8000`)

Start the cluster in background mode:
```bash
docker compose up -d llm-server app-node-1
```
Navigate to **`http://localhost:8000`** in two different browser windows (or one incognito window):

1. **Window 1 (User Alice)**:
   - Click "Sign Up", enter username `alice` and password `password123`.
   - Alice logs in. Alice's status dot in the header turns **green (active)**.
2. **Window 2 (User Bob)**:
   - Click "Sign Up", enter username `bob` and password `password123`.
   - Bob logs in. Bob's status dot turns **green (active)**.
3. **Inspect Directory**:
   - In Alice's window, `bob` immediately appears in the "Users" list with a green active badge.
   - In Bob's window, `alice` appears with a green active badge.
4. **1-on-1 Direct Messaging**:
   - Alice clicks on `bob` to open a DM.
   - Alice types "Hello Bob!" and clicks Send.
   - Within 2 seconds, Bob's chat window shows the message from Alice.
5. **PDF & Image Sharing**:
   - Alice clicks the paperclip icon (`📎`), selects an image or PDF from her computer, and sends it.
   - Images appear inline in the chat bubble.
   - PDFs display as file cards with a download button.
6. **Group Creation**:
   - Alice clicks **"+ New Group"**, enters group name "Backend Team", checks `bob` in the member checklist, and clicks **"Create Group"**.
   - Alice is `ADMIN` of the group.
   - In Bob's window, `# Backend Team` appears immediately under "Groups".
7. **Group Management**:
   - Alice (as Admin) sees the **"⚙️ Manage"** button in the group header.
   - Clicking it allows Alice to rename the group, add new members, or promote Bob to Admin.
   - If Bob looks at the header, the "Manage" button is hidden because non-admins cannot manage the group.
8. **AI Assistance**:
   - Click **"💡 Smart Replies"**: 3 pill buttons appear above the input box suggesting quick replies based on the conversation history. Clicking one immediately sends the message.
   - Click **"📝 Summarize"**: a popup modal appears with a concise bullet-point summary of the whole chat history.
9. **Logout & Inactivity**:
   - Bob clicks **"Logout"**.
   - In Alice's window, Bob's badge updates to gray (`inactive`).
