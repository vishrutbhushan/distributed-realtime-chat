/**
 * Web UI Application Logic & DOM Handlers
 */

// Application State
let authMode = "login";
let currentUser = null; // { token, user_id, username }
let currentChat = null; // { type: 'DM'|'GROUP', id, name, status?, role? }
let cachedUsers = [];
let cachedGroups = [];
let currentMessages = [];
let pendingFile = null;
let eventController = null;
let eventGeneration = 0;
let reconnectTimer = null;
let reconnectResolver = null;
let reconnectBaseMs = 1000;
let snapshotInProgress = false;
let anotherSnapshotRequested = false;
let resyncAfterSnapshot = false;
let bufferedStreamEvents = [];
let directoryLoadPromise = null;
let directoryDirtyDuringLoad = false;
let directoryRefreshTimer = null;
let historyAbortController = null;
let historyRequestSequence = 0;
let lastSmartReplyMsgId = null;
let pageSuspended = false;

function switchTab(mode) {
  authMode = mode;
  document.getElementById("tab-login").classList.toggle("active", mode === "login");
  document.getElementById("tab-signup").classList.toggle("active", mode === "signup");
  document.getElementById("auth-submit-btn").innerText = mode === "login" ? "Log In" : "Sign Up";
  document.getElementById("auth-error").innerText = "";
}

async function submitAuth() {
  const u = document.getElementById("auth-username").value.trim();
  const p = document.getElementById("auth-password").value;
  const err = document.getElementById("auth-error");
  err.innerText = "";

  if (!u || !p) {
    err.innerText = "Please enter username and password.";
    return;
  }

  try {
    const data = authMode === "login" ? await api.login(u, p) : await api.signup(u, p);
    if (data.success) {
      currentUser = { token: data.token, user_id: data.user_id, username: data.username };
      sessionStorage.setItem("chat_session", JSON.stringify(currentUser));
      document.getElementById("auth-container").style.display = "none";
      document.getElementById("current-username").innerText = data.username;
      startEventStream();
    } else {
      err.innerText = data.message || "Authentication failed.";
    }
  } catch (e) {
    err.innerText = "Connection error. Ensure Docker backend is running.";
  }
}

async function logout() {
  if (currentUser) {
    try {
      await api.logout(currentUser.token);
    } catch (e) {}
  }
  sessionStorage.removeItem("chat_session");
  sessionStorage.removeItem("chat_current_chat");
  stopEventStream();
  if (historyAbortController) historyAbortController.abort();
  historyAbortController = null;
  historyRequestSequence += 1;
  lastSmartReplyMsgId = null;
  clearSmartReplySuggestions();
  if (directoryRefreshTimer) clearTimeout(directoryRefreshTimer);
  directoryRefreshTimer = null;
  directoryLoadPromise = null;
  directoryDirtyDuringLoad = false;
  currentUser = null;
  currentChat = null;
  currentMessages = [];
  cachedUsers = [];
  cachedGroups = [];
  renderUsersList(cachedUsers);
  renderGroupsList(cachedGroups);
  document.getElementById("auth-container").style.display = "flex";
  document.getElementById("auth-password").value = "";
  document.getElementById("chat-title").innerText = "Select a conversation";
  document.getElementById("chat-subtitle").innerText = "Choose a user or group to start collaborating";
  document.getElementById("chat-actions").style.display = "none";
  document.getElementById("input-area").style.display = "none";
  document.getElementById("messages-container").innerHTML = `<div class="empty-chat">Select a user or group from the sidebar to start chatting.</div>`;
}

function stopEventStream() {
  eventGeneration += 1;
  if (eventController) eventController.abort();
  eventController = null;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = null;
  if (reconnectResolver) reconnectResolver();
  reconnectResolver = null;
  snapshotInProgress = false;
  anotherSnapshotRequested = false;
  resyncAfterSnapshot = false;
  bufferedStreamEvents = [];
}

function startEventStream() {
  stopEventStream();
  const generation = eventGeneration;
  reconnectBaseMs = 1000;
  runEventStream(generation);
}

async function runEventStream(generation) {
  let retryMs = 1000;
  while (currentUser && !pageSuspended && generation === eventGeneration) {
    const controller = new AbortController();
    eventController = controller;
    const openedAt = Date.now();
    let readyReceived = false;

    try {
      const response = await fetch(api.eventStreamUrl(currentUser.token), {
        method: "GET",
        headers: { Accept: "text/event-stream" },
        cache: "no-store",
        signal: controller.signal,
      });
      if (response.status === 401) {
        if (generation === eventGeneration) await logout();
        return;
      }
      if (!response.ok || !response.body) {
        throw new Error(`Event stream returned HTTP ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (generation === eventGeneration) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");
        let boundary;
        while ((boundary = buffer.indexOf("\n\n")) !== -1) {
          const frame = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const event = parseSseFrame(frame);
          if (!event) continue;
          if (event.type === "READY") readyReceived = true;
          handleRealtimeEvent(event.type, event.data, generation);
        }
      }
      if (generation !== eventGeneration) return;
      if (!readyReceived) throw new Error("Event stream closed before READY");
    } catch (e) {
      if (generation !== eventGeneration || e.name === "AbortError") return;
      console.warn("Event stream disconnected; reconnecting", e);
    } finally {
      if (eventController === controller) eventController = null;
    }

    if (generation !== eventGeneration || !currentUser || pageSuspended) return;
    if (Date.now() - openedAt >= 30000) retryMs = 1000;
    const jitteredMs = Math.max(1000, Math.floor(retryMs * (0.75 + Math.random() * 0.5)));
    await new Promise(resolve => {
      reconnectResolver = resolve;
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        reconnectResolver = null;
        resolve();
      }, jitteredMs);
    });
    retryMs = Math.min(30000, Math.round(retryMs * 1.8));
  }
}

function parseSseFrame(frame) {
  let type = "message";
  const dataLines = [];
  for (const line of frame.split("\n")) {
    if (!line || line.startsWith(":")) continue;
    const separator = line.indexOf(":");
    const field = separator === -1 ? line : line.slice(0, separator);
    const value = separator === -1 ? "" : line.slice(separator + 1).replace(/^ /, "");
    if (field === "event") type = value;
    else if (field === "data") dataLines.push(value);
  }
  if (dataLines.length === 0) return null;
  try {
    return { type, data: JSON.parse(dataLines.join("\n")) };
  } catch (e) {
    console.warn("Ignoring malformed event stream frame", e);
    return null;
  }
}

function handleRealtimeEvent(type, data, generation) {
  if (generation !== eventGeneration) return;
  if (type === "READY") {
    if (snapshotInProgress) {
      anotherSnapshotRequested = true;
      return;
    }
    beginSnapshotReconciliation(generation);
    return;
  }
  if (type === "HEARTBEAT") return;
  if (type === "AUTH_EXPIRED") {
    logout();
    return;
  }
  if (type === "GROUP_ACCESS_REVOKED") {
    if (currentChat && currentChat.type === "GROUP" && currentChat.id === data?.group_id) {
      clearSelectedConversation("Your access to this group was removed.");
    }
    requestDirectoryRefresh();
    return;
  }
  if (snapshotInProgress) {
    if (type === "RESYNC_REQUIRED") resyncAfterSnapshot = true;
    else bufferedStreamEvents.push({ type, data });
    return;
  }
  if (type === "RESYNC_REQUIRED") {
    beginSnapshotReconciliation(generation);
    return;
  }
  applyRealtimeEvent(type, data);
}

function beginSnapshotReconciliation(generation) {
  if (snapshotInProgress || generation !== eventGeneration) return;
  snapshotInProgress = true;
  bufferedStreamEvents = [];
  const tasks = [loadDirectory()];
  if (currentChat) tasks.push(loadMessages({ snapshot: true }));

  Promise.all(tasks).finally(() => {
    if (generation !== eventGeneration) return;
    const buffered = bufferedStreamEvents;
    const repeat = anotherSnapshotRequested || resyncAfterSnapshot ||
      buffered.some(event => event.type === "RESYNC_REQUIRED");
    snapshotInProgress = false;
    anotherSnapshotRequested = false;
    resyncAfterSnapshot = false;
    bufferedStreamEvents = [];
    buffered.forEach(event => {
      if (event.type !== "RESYNC_REQUIRED") applyRealtimeEvent(event.type, event.data);
    });
    if (repeat) beginSnapshotReconciliation(generation);
  });
}

function applyRealtimeEvent(type, data) {
  if (type === "DIRECTORY_CHANGED") {
    requestDirectoryRefresh();
    return;
  }
  if (type !== "NEW_MESSAGE" || !data?.message) return;

  const message = data.message;
  requestDirectoryRefresh();
  const key = conversationKeyForMessage(message);
  if (key !== conversationKey()) return;

  mergeIntoCurrentMessages([message]);
  renderMessages(currentMessages);
  maybeRequestSmartReplies(message);
  if (message.sender_id !== currentUser.user_id && currentChat) {
    api.markRead(currentUser.token, currentChat.type, currentChat.id).catch(() => {});
  }
}

function conversationKey(chat = currentChat) {
  return chat ? `${chat.type}:${chat.id}` : "";
}

function conversationKeyForMessage(message) {
  if (message.chat_type === "GROUP") return `GROUP:${message.group_id}`;
  const otherUserId = message.sender_id === currentUser?.user_id
    ? message.recipient_id
    : message.sender_id;
  return `DM:${otherUserId}`;
}

function mergeIntoCurrentMessages(messages) {
  const byId = new Map(currentMessages.map(message => [message.message_id, message]));
  messages.forEach(message => {
    if (message?.message_id) byId.set(message.message_id, { ...byId.get(message.message_id), ...message });
  });
  currentMessages = [...byId.values()]
    .sort((a, b) => (a.timestamp - b.timestamp) || a.message_id.localeCompare(b.message_id))
    .slice(-100);
}

function clearSmartReplySuggestions() {
  const bar = document.getElementById("smart-replies-bar");
  if (!bar) return;
  bar.innerHTML = "";
  bar.style.display = "none";
}

function maybeRequestSmartReplies(message) {
  if (!message || !currentUser) return;
  if (message.sender_id !== currentUser.user_id && message.message_id !== lastSmartReplyMsgId) {
    lastSmartReplyMsgId = message.message_id;
    clearSmartReplySuggestions();
    if (hasSmartReplyText(message)) requestSmartReplies(message.message_id);
  } else if (message.sender_id === currentUser.user_id) {
    lastSmartReplyMsgId = null;
    clearSmartReplySuggestions();
  }
}

function clearSelectedConversation(message) {
  if (historyAbortController) historyAbortController.abort();
  historyAbortController = null;
  historyRequestSequence += 1;
  currentChat = null;
  currentMessages = [];
  sessionStorage.removeItem("chat_current_chat");
  document.getElementById("chat-title").innerText = "Select a conversation";
  document.getElementById("chat-subtitle").innerText = message || "Choose a user or group to start collaborating";
  document.getElementById("chat-actions").style.display = "none";
  document.getElementById("input-area").style.display = "none";
  document.getElementById("messages-container").innerHTML =
    `<div class="empty-chat">${escapeHtml(message || "Select a user or group from the sidebar to start chatting.")}</div>`;
  renderUsersList(cachedUsers);
  renderGroupsList(cachedGroups);
}

function requestDirectoryRefresh() {
  if (directoryRefreshTimer) return;
  directoryRefreshTimer = setTimeout(() => {
    directoryRefreshTimer = null;
    loadDirectory();
  }, 40);
}

async function loadDirectory() {
  if (!currentUser) return;
  if (directoryLoadPromise) {
    directoryDirtyDuringLoad = true;
    return directoryLoadPromise;
  }

  const token = currentUser.token;
  const loadPromise = (async () => {
    do {
      directoryDirtyDuringLoad = false;
      try {
        const [uResp, gResp] = await Promise.all([
          api.getUsers(token),
          api.getGroups(token),
        ]);

        if (currentUser?.token !== token) return;
        // If unauthorized or token expired, reset session cleanly.
        if ((!uResp.success && (uResp.error?.includes("token") || uResp.error?.includes("UNAUTHENTICATED"))) ||
            (!gResp.success && (gResp.error?.includes("token") || gResp.error?.includes("UNAUTHENTICATED")))) {
          logout();
          return;
        }

        if (uResp.success) {
          cachedUsers = uResp.users;
          if (currentChat && currentChat.type === "DM") {
            const u = cachedUsers.find(x => x.user_id === currentChat.id);
            if (u) {
              u.unread_count = 0;
              if (u.status !== currentChat.status) {
                currentChat.status = u.status;
                sessionStorage.setItem("chat_current_chat", JSON.stringify(currentChat));
                updateChatHeader();
              }
            }
          }
          renderUsersList(cachedUsers);
        }

        if (gResp.success) {
          cachedGroups = gResp.groups;
          if (currentChat && currentChat.type === "GROUP") {
            const g = cachedGroups.find(x => x.group_id === currentChat.id);
            if (!g) {
              clearSelectedConversation("Your access to this group was removed.");
            } else {
              g.unread_count = 0;
              if (g.name !== currentChat.name || g.member_count !== currentChat.member_count || g.user_role !== currentChat.role) {
                currentChat.name = g.name;
                currentChat.member_count = g.member_count;
                currentChat.role = g.user_role;
                sessionStorage.setItem("chat_current_chat", JSON.stringify(currentChat));
                updateChatHeader();
              }
            }
          }
          renderGroupsList(cachedGroups);
        }
      } catch (e) {
        console.error("Directory load error", e);
      }

    } while (directoryDirtyDuringLoad && currentUser?.token === token);
  })();
  directoryLoadPromise = loadPromise;
  try {
    await loadPromise;
  } finally {
    if (directoryLoadPromise === loadPromise) directoryLoadPromise = null;
  }
}

function renderUsersList(users) {
  const list = document.getElementById("users-list");
  list.innerHTML = "";
  if (users.length === 0) {
    list.innerHTML = `<li style="font-size: 12px; color: var(--text-muted); padding: 8px;">No other users registered.</li>`;
    return;
  }

  // Sort users in real-time: Most recently active chat moves to the top
  const sortedUsers = [...users].sort((a, b) => {
    const timeA = a.last_message_time || 0;
    const timeB = b.last_message_time || 0;
    if (timeB !== timeA) return timeB - timeA;
    if (a.status === "active" && b.status !== "active") return -1;
    if (b.status === "active" && a.status !== "active") return 1;
    return a.username.localeCompare(b.username);
  });

  sortedUsers.forEach(u => {
    const isSelected = currentChat && currentChat.type === "DM" && currentChat.id === u.user_id;
    const unread = isSelected ? 0 : (u.unread_count || 0);

    const li = document.createElement("li");
    li.className = "item" + (isSelected ? " selected" : "");
    li.onclick = () => selectDM(u);
    li.innerHTML = `
      <div class="status-dot ${u.status === 'active' ? 'active' : ''}"></div>
      <div class="item-info">
        <div class="item-header-row">
          <span class="item-name">${escapeHtml(u.username)}</span>
          ${unread > 0 
            ? `<span class="unread-badge">${unread > 99 ? '99+' : unread}</span>`
            : `<span class="item-badge">${u.status}</span>`
          }
        </div>
        ${u.last_message ? `<div class="item-preview">${escapeHtml(u.last_message)}</div>` : ''}
      </div>
    `;
    list.appendChild(li);
  });
}

function renderGroupsList(groups) {
  const list = document.getElementById("groups-list");
  list.innerHTML = "";
  if (groups.length === 0) {
    list.innerHTML = `<li style="font-size: 12px; color: var(--text-muted); padding: 8px;">No groups yet.</li>`;
    return;
  }

  // Sort groups in real-time: Most recently active group moves to the top
  const sortedGroups = [...groups].sort((a, b) => {
    const timeA = a.last_message_time || (a.created_at * 1000) || 0;
    const timeB = b.last_message_time || (b.created_at * 1000) || 0;
    if (timeB !== timeA) return timeB - timeA;
    return a.name.localeCompare(b.name);
  });

  sortedGroups.forEach(g => {
    const isSelected = currentChat && currentChat.type === "GROUP" && currentChat.id === g.group_id;
    const unread = isSelected ? 0 : (g.unread_count || 0);

    const li = document.createElement("li");
    li.className = "item" + (isSelected ? " selected" : "");
    li.onclick = () => selectGroup(g);
    li.innerHTML = `
      <span style="color: var(--primary); font-weight: bold;">#</span>
      <div class="item-info">
        <div class="item-header-row">
          <span class="item-name">${escapeHtml(g.name)}</span>
          ${unread > 0 
            ? `<span class="unread-badge">${unread > 99 ? '99+' : unread}</span>`
            : `<span class="item-badge">${g.member_count} mem</span>`
          }
        </div>
        ${g.last_message ? `<div class="item-preview">${escapeHtml(g.last_message)}</div>` : ''}
      </div>
    `;
    list.appendChild(li);
  });
}

function selectDM(u) {
  lastSmartReplyMsgId = null;
  clearSmartReplySuggestions();
  currentChat = { type: "DM", id: u.user_id, name: u.username, status: u.status };
  sessionStorage.setItem("chat_current_chat", JSON.stringify(currentChat));
  currentMessages = [];
  renderMessages(currentMessages);

  // Instantly clear unread count locally for instant UI update
  u.unread_count = 0;
  const match = cachedUsers.find(x => x.user_id === u.user_id);
  if (match) match.unread_count = 0;
  renderUsersList(cachedUsers);

  if (currentUser) {
    api.markRead(currentUser.token, "DM", u.user_id).catch(() => {});
  }

  updateChatHeader();
  loadMessages();
}

function selectGroup(g) {
  lastSmartReplyMsgId = null;
  clearSmartReplySuggestions();
  currentChat = { type: "GROUP", id: g.group_id, name: g.name, role: g.user_role, member_count: g.member_count };
  sessionStorage.setItem("chat_current_chat", JSON.stringify(currentChat));
  currentMessages = [];
  renderMessages(currentMessages);

  // Instantly clear unread count locally for instant UI update
  g.unread_count = 0;
  const match = cachedGroups.find(x => x.group_id === g.group_id);
  if (match) match.unread_count = 0;
  renderGroupsList(cachedGroups);

  if (currentUser) {
    api.markRead(currentUser.token, "GROUP", g.group_id).catch(() => {});
  }

  updateChatHeader();
  loadMessages();
}

function updateChatHeader() {
  if (!currentChat) return;
  document.getElementById("chat-actions").style.display = "flex";
  document.getElementById("input-area").style.display = "flex";
  document.getElementById("smart-replies-bar").style.display = "none";

  const title = document.getElementById("chat-title");
  const sub = document.getElementById("chat-subtitle");
  const dot = document.getElementById("header-status-dot");
  const manageBtn = document.getElementById("btn-manage-group");
  const addMemberBtn = document.getElementById("btn-add-member");

  if (currentChat.type === "DM") {
    title.innerText = currentChat.name;
    sub.innerText = `Direct Message (${currentChat.status})`;
    dot.style.display = "block";
    dot.className = "status-dot " + (currentChat.status === "active" ? "active" : "");
    if (manageBtn) manageBtn.style.display = "none";
    if (addMemberBtn) addMemberBtn.style.display = "none";
  } else {
    title.innerText = "# " + currentChat.name;
    sub.innerText = `Group · ${currentChat.member_count || 1} members · You are ${currentChat.role}`;
    dot.style.display = "none";
    const isAdmin = currentChat.role === "ADMIN";
    if (manageBtn) manageBtn.style.display = isAdmin ? "inline-flex" : "none";
    if (addMemberBtn) addMemberBtn.style.display = "none";
  }
  renderUsersList(cachedUsers);
  renderGroupsList(cachedGroups);
}

async function loadMessages(options = {}) {
  if (!currentChat || !currentUser) return;
  const chat = { ...currentChat };
  const key = conversationKey(chat);
  const token = currentUser.token;
  if (historyAbortController) historyAbortController.abort();
  const controller = new AbortController();
  historyAbortController = controller;
  const requestSequence = ++historyRequestSequence;

  try {
    const data = chat.type === "DM"
      ? await api.getDMs(token, chat.id, controller.signal)
      : await api.getGroupMessages(token, chat.id, controller.signal);

    if (controller.signal.aborted || requestSequence !== historyRequestSequence ||
        currentUser?.token !== token || conversationKey() !== key) return;

    if (data.success) {
      // Merge the authoritative latest-100 snapshot with any live events that
      // arrived while it was in flight. Message IDs make the merge idempotent.
      const liveMessages = currentMessages;
      currentMessages = [];
      mergeIntoCurrentMessages([...(data.messages || []), ...liveMessages]);
      renderMessages(currentMessages);

      if (currentMessages.length > 0) {
        const latest = currentMessages[currentMessages.length - 1];
        if (chat.type === "DM") {
          const u = cachedUsers.find(x => x.user_id === chat.id);
          if (u && (u.last_message_time !== latest.timestamp || u.unread_count !== 0)) {
            u.last_message_time = latest.timestamp;
            u.last_message = latest.content || (latest.file_id ? '[File Attachment]' : '');
            u.unread_count = 0;
            renderUsersList(cachedUsers);
          }
        } else {
          const g = cachedGroups.find(x => x.group_id === chat.id);
          if (g && (g.last_message_time !== latest.timestamp || g.unread_count !== 0)) {
            g.last_message_time = latest.timestamp;
            g.last_message = latest.content || (latest.file_id ? '[File Attachment]' : '');
            g.unread_count = 0;
            renderGroupsList(cachedGroups);
          }
        }
      }

      if (currentMessages.length > 0) {
        maybeRequestSmartReplies(currentMessages[currentMessages.length - 1]);
      } else {
        lastSmartReplyMsgId = null;
        clearSmartReplySuggestions();
      }
    }
  } catch (e) {
    if (e.name !== "AbortError") console.error("Messages load error", e);
  } finally {
    if (historyAbortController === controller) historyAbortController = null;
  }
}

function renderMessages(messages) {
  const container = document.getElementById("messages-container");
  container.innerHTML = "";
  if (messages.length === 0) {
    container.innerHTML = `<div class="empty-chat">No messages yet. Send a message to start!</div>`;
    return;
  }

  messages.forEach(m => {
    const isMe = m.sender_id === currentUser.user_id;
    const row = document.createElement("div");
    row.className = `message-bubble ${isMe ? "mine" : "other"}`;

    const senderLabel = isMe ? "" : `<div class="msg-sender">${escapeHtml(m.sender_username)}</div>`;
    let fileHtml = "";

    if (m.file_id) {
      if (m.file_type === "image") {
        fileHtml = `<a href="/api/files/download?file_id=${encodeURIComponent(m.file_id)}&token=${encodeURIComponent(currentUser.token)}" target="_blank">
                      <img class="msg-image" src="/api/files/download?file_id=${encodeURIComponent(m.file_id)}&token=${encodeURIComponent(currentUser.token)}" alt="${escapeHtml(m.filename)}">
                    </a>`;
      } else {
        fileHtml = `<a class="msg-file-card" href="/api/files/download?file_id=${encodeURIComponent(m.file_id)}&token=${encodeURIComponent(currentUser.token)}" target="_blank">
                      <span>📄</span>
                      <span>${escapeHtml(m.filename)} (${formatBytes(m.file_size)})</span>
                      <span style="color: var(--primary); margin-left: auto;">Download</span>
                    </a>`;
      }
    }

    const contentHtml = m.content ? `<div>${escapeHtml(m.content)}</div>` : "";
    const timeStr = new Date(m.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

    row.innerHTML = `
      ${senderLabel}
      <div class="msg-content">
        ${contentHtml}
        ${fileHtml}
      </div>
      <div class="msg-time">${timeStr}</div>
    `;
    container.appendChild(row);
  });
  container.scrollTop = container.scrollHeight;
}

function onFileSelected(input) {
  if (input.files && input.files[0]) {
    pendingFile = input.files[0];
    document.getElementById("preview-filename").innerText = `${pendingFile.name} (${formatBytes(pendingFile.size)})`;
    document.getElementById("file-preview-bar").style.display = "flex";
  }
}

function clearFileAttachment() {
  pendingFile = null;
  document.getElementById("file-input").value = "";
  document.getElementById("file-preview-bar").style.display = "none";
}

async function sendMessage() {
  if (!currentChat || !currentUser) return;
  const chat = { ...currentChat };
  const chatKey = conversationKey(chat);
  const token = currentUser.token;
  const outgoingFile = pendingFile;
  const textInput = document.getElementById("message-input");
  const content = textInput.value.trim();

  if (!content && !pendingFile) return;

  let fileId = "";
  // Upload file first if attached
  if (outgoingFile) {
    try {
      const upData = await api.uploadFile(token, chat.type, chat.id, outgoingFile);
      if (upData.success) {
        fileId = upData.file.file_id;
      } else {
        alert("File upload failed: " + (upData.message || upData.error));
        return;
      }
    } catch (e) {
      alert("Error uploading file: " + e.message);
      return;
    }
  }

  try {
    const data = chat.type === "DM"
      ? await api.sendDM(token, chat.id, content, fileId)
      : await api.sendGroupMessage(token, chat.id, content, fileId);

    if (data.success) {
      if (conversationKey() === chatKey) {
        textInput.value = "";
        clearFileAttachment();
        lastSmartReplyMsgId = null;
        clearSmartReplySuggestions();
        if (data.message) {
          mergeIntoCurrentMessages([data.message]);
          renderMessages(currentMessages);
        }
      }

      const now = Date.now();
      const previewText = content || (fileId ? '[File Attachment]' : '');
      if (chat.type === "DM") {
        const u = cachedUsers.find(x => x.user_id === chat.id);
        if (u) {
          u.last_message_time = now;
          u.last_message = previewText;
          u.unread_count = 0;
          renderUsersList(cachedUsers);
        }
      } else {
        const g = cachedGroups.find(x => x.group_id === chat.id);
        if (g) {
          g.last_message_time = now;
          g.last_message = previewText;
          g.unread_count = 0;
          renderGroupsList(cachedGroups);
        }
      }

      requestDirectoryRefresh();
    } else {
      alert(data.error || "Failed to send message");
    }
  } catch (e) {
    alert("Error sending message: " + e.message);
  }
}

// AI LLM Features
async function requestSmartReplies(messageId) {
  if (!currentChat || !currentUser) return;
  const chat = { ...currentChat };
  const token = currentUser.token;
  const chatKey = conversationKey(chat);
  const messages = [...currentMessages];
  const latestMessage = messages[messages.length - 1];
  if (!latestMessage || latestMessage.message_id !== messageId || !String(latestMessage.content || "").trim()) return;

  const requestIdentity = { token, chatKey, messageId };
  const historyStrings = messages
    .filter(m => String(m.content || "").trim())
    .map(m => `${m.sender_username || "User"}: ${m.content}`);
  const isStillCurrent = () => isSmartReplyRequestCurrent(requestIdentity, {
    token: currentUser?.token,
    chatKey: conversationKey(),
    messageId: currentMessages[currentMessages.length - 1]?.message_id,
  }) && lastSmartReplyMsgId === messageId;

  try {
    const data = await api.smartReplies(
      token,
      chat.type,
      chat.id,
      historyStrings,
      latestMessage.content,
      chat.name
    );
    if (!isStillCurrent()) return;
    if (data.success && Array.isArray(data.suggestions) && data.suggestions.length === 3) {
      const bar = document.getElementById("smart-replies-bar");
      if (!bar) return;
      bar.innerHTML = "";
      data.suggestions.forEach(s => {
        const btn = document.createElement("button");
        btn.className = "reply-pill";
        btn.innerText = s;
        btn.onclick = () => {
          document.getElementById("message-input").value = s;
          sendMessage();
        };
        bar.appendChild(btn);
      });
      bar.style.display = "flex";
    } else {
      clearSmartReplySuggestions();
    }
  } catch (e) {
    if (isStillCurrent()) {
      clearSmartReplySuggestions();
      console.error("Smart replies error", e);
    }
  }
}

async function requestSummarize() {
  if (!currentChat || !currentUser) return;
  openModal("summary-modal");
  document.getElementById("summary-text").innerText = "Generating summary over entire conversation history...";

  let historyStrings = currentMessages.map(m => `${m.sender_username || "User"}: ${m.content || ""}`).filter(s => s.trim().length > 0);
  if (historyStrings.length === 0) {
    try {
      await loadMessages({ snapshot: true });
      historyStrings = currentMessages.map(m => `${m.sender_username || "User"}: ${m.content || ""}`).filter(s => s.trim().length > 0);
    } catch (_) {}
  }

  try {
    const data = await api.summarize(
      currentUser.token,
      currentChat.type,
      currentChat.id,
      historyStrings,
      currentChat.name,
      currentUser.username
    );
    if (!data.success && data.error) {
      document.getElementById("summary-text").innerText = "Could not generate summary: " + data.error;
    } else {
      document.getElementById("summary-text").innerText = data.summary || "No summary available.";
    }
  } catch (e) {
    document.getElementById("summary-text").innerText = "Error requesting summary: " + e.message;
  }
}

async function requestSuggestion() {
  if (!currentChat || !currentUser) return;
  openModal("suggestion-modal");
  document.getElementById("suggestion-text").innerText = "Generating context-aware suggestion from conversation history...";

  const historyStrings = currentMessages.map(m => `${m.sender_username}: ${m.content}`);
  try {
    const data = await api.suggest(
      currentUser.token,
      currentChat.type,
      currentChat.id,
      historyStrings,
      currentChat.name
    );
    document.getElementById("suggestion-text").innerText = data.suggestion || "No suggestion available.";
  } catch (e) {
    document.getElementById("suggestion-text").innerText = "Error requesting suggestion: " + e.message;
  }
}

// Group Management
function openCreateGroupModal() {
  const container = document.getElementById("create-group-user-list");
  container.innerHTML = "";
  if (cachedUsers.length === 0) {
    container.innerHTML = `<div style="font-size: 12px; color: var(--text-muted);">No other users to select.</div>`;
  } else {
    cachedUsers.forEach(u => {
      const div = document.createElement("div");
      div.className = "user-select-item";
      div.innerHTML = `
        <input type="checkbox" id="chk-${u.user_id}" value="${u.user_id}">
        <label for="chk-${u.user_id}">${escapeHtml(u.username)} (${u.status})</label>
      `;
      container.appendChild(div);
    });
  }
  openModal("create-group-modal");
}

async function submitCreateGroup() {
  const name = document.getElementById("new-group-name").value.trim();
  if (!name) { alert("Please enter a group name."); return; }

  const checkboxes = document.querySelectorAll("#create-group-user-list input[type='checkbox']:checked");
  const memberIds = Array.from(checkboxes).map(c => c.value);

  try {
    const data = await api.createGroup(currentUser.token, name, memberIds);
    if (data.success) {
      closeModal("create-group-modal");
      document.getElementById("new-group-name").value = "";
      loadDirectory();
      selectGroup(data.group);
    } else {
      alert(data.message || "Failed to create group");
    }
  } catch (e) {
    alert("Error creating group: " + e.message);
  }
}

async function openManageGroupModal() {
  if (!currentChat || currentChat.type !== "GROUP") return;
  document.getElementById("rename-group-name").value = currentChat.name;

  try {
    const [data, usersData] = await Promise.all([
      api.getGroupMembers(currentUser.token, currentChat.id),
      api.getUsers(currentUser.token),
    ]);

    if (usersData && usersData.success) {
      cachedUsers = usersData.users || [];
    }

    if (data.success) {
      const list = document.getElementById("manage-members-list");
      list.innerHTML = "";
      const memberIds = new Set();

      data.members.forEach(m => {
        memberIds.add(m.user_id);
        const div = document.createElement("div");
        div.className = "member-item-row";
        const isAdmin = m.role === "ADMIN";
        const isMe = m.user_id === currentUser.user_id;

        div.innerHTML = `
          <div style="display: flex; align-items: center; gap: 6px; min-width: 0; flex: 1;">
            <span style="font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${escapeHtml(m.username)}</span>
            <span class="role-badge ${isAdmin ? 'admin' : ''}">${m.role}</span>
          </div>
          <div style="display: flex; gap: 4px; flex-shrink: 0;">
            ${!isAdmin ? `<button class="btn-action-xs" onclick="groupAction('MAKE_ADMIN', '${m.user_id}')">Admin</button>` : ''}
            ${!isMe ? `<button class="btn-action-xs danger" onclick="groupAction('REMOVE_MEMBER', '${m.user_id}')">Remove</button>` : ''}
          </div>
        `;
        list.appendChild(div);
      });

      // Fill add member select with all registered users who aren't yet in this group
      const sel = document.getElementById("add-member-select");
      const availableUsers = cachedUsers.filter(u => !memberIds.has(u.user_id));
      if (availableUsers.length === 0) {
        sel.innerHTML = `<option value="">All registered users are already members</option>`;
      } else {
        sel.innerHTML = `<option value="">Select user to add...</option>`;
        availableUsers.forEach(u => {
          sel.innerHTML += `<option value="${u.user_id}">${escapeHtml(u.username)} (${u.status})</option>`;
        });
      }
    }
  } catch (e) {
    alert("Error loading group members: " + e.message);
  }

  openModal("manage-group-modal");
}

async function groupAction(action, targetUserId, newName = null) {
  try {
    const data = await api.updateGroup(currentUser.token, currentChat.id, action, targetUserId, newName);
    if (data.success) {
      if (newName) currentChat.name = newName;
      openManageGroupModal();
      loadDirectory();
      updateChatHeader();
    } else {
      alert(data.message || "Action failed");
    }
  } catch (e) {
    alert("Action error: " + e.message);
  }
}

function submitRenameGroup() {
  const newName = document.getElementById("rename-group-name").value.trim();
  if (newName) groupAction("RENAME", null, newName);
}

function submitAddMember() {
  const sel = document.getElementById("add-member-select");
  const uid = sel.value;
  if (!uid) {
    alert("Please select a user to add to the group.");
    return;
  }
  groupAction("ADD_MEMBER", uid);
}

// Modal helpers
function openModal(id) { document.getElementById(id).style.display = "flex"; }
function closeModal(id) { document.getElementById(id).style.display = "none"; }

function formatBytes(bytes) {
  if (!bytes) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

function escapeHtml(str) {
  if (!str) return "";
  return String(str).replace(/[&<>"']/g, s => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[s]));
}

function initSession() {
  const uInput = document.getElementById("auth-username");
  const pInput = document.getElementById("auth-password");
  if (uInput && !uInput._hasEnter) {
    uInput.addEventListener("keydown", e => { if (e.key === "Enter") submitAuth(); });
    uInput._hasEnter = true;
  }
  if (pInput && !pInput._hasEnter) {
    pInput.addEventListener("keydown", e => { if (e.key === "Enter") submitAuth(); });
    pInput._hasEnter = true;
  }

  const saved = sessionStorage.getItem("chat_session");
  if (saved) {
    try {
      const parsed = JSON.parse(saved);
      if (parsed && parsed.token && parsed.user_id && parsed.username) {
        currentUser = parsed;
        document.getElementById("auth-container").style.display = "none";
        document.getElementById("current-username").innerText = parsed.username;

        const savedChat = sessionStorage.getItem("chat_current_chat");
        if (savedChat) {
          try {
            currentChat = JSON.parse(savedChat);
            updateChatHeader();
          } catch (e) {
            currentChat = null;
          }
        }

        startEventStream();
        return;
      }
    } catch (e) {
      sessionStorage.removeItem("chat_session");
    }
  }

  document.getElementById("auth-container").style.display = "flex";
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initSession);
} else {
  initSession();
}

window.addEventListener("pagehide", () => {
  pageSuspended = true;
  stopEventStream();
});

window.addEventListener("pageshow", () => {
  pageSuspended = false;
  if (currentUser && !eventController) startEventStream();
});

