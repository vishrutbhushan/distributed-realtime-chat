/**
 * HTTP REST API Client for the Distributed Real-time Chat Gateway
 */

const API_BASE = "";

function getTabSessionId() {
  let sess = sessionStorage.getItem("tab_chat_session");
  if (!sess) {
    sess = "tab-" + (typeof crypto !== "undefined" && crypto.randomUUID ? crypto.randomUUID() : (Date.now().toString(36) + Math.random().toString(36).slice(2)));
    sessionStorage.setItem("tab_chat_session", sess);
  }
  return sess;
}

function resetTabSessionId() {
  const sess = "tab-" + (typeof crypto !== "undefined" && crypto.randomUUID ? crypto.randomUUID() : (Date.now().toString(36) + Math.random().toString(36).slice(2)));
  sessionStorage.setItem("tab_chat_session", sess);
  return sess;
}

async function fetchWithFailover(url, options = {}, retries = 2) {
  const customHeaders = {
    "X-Chat-Session": getTabSessionId(),
  };
  const headers = Object.assign({}, customHeaders, options.headers || {});
  const mergedOptions = Object.assign({}, options, { headers });
  for (let i = 0; i <= retries; i++) {
    try {
      const res = await fetch(url, mergedOptions);
      if (res.status >= 502 && i < retries) {
        await new Promise(r => setTimeout(r, 250 * (i + 1)));
        continue;
      }
      return res;
    } catch (err) {
      if (err.name === "AbortError" || i === retries) throw err;
      await new Promise(r => setTimeout(r, 250 * (i + 1)));
    }
  }
}

const api = {
  getTabSessionId() {
    return getTabSessionId();
  },

  resetTabSessionId() {
    return resetTabSessionId();
  },

  async signup(username, password) {
    resetTabSessionId();
    const res = await fetchWithFailover(`${API_BASE}/api/signup`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
    return res.json();
  },

  async login(username, password) {
    resetTabSessionId();
    const res = await fetchWithFailover(`${API_BASE}/api/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
    return res.json();
  },

  async logout(token) {
    const res = await fetchWithFailover(`${API_BASE}/api/logout`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token }),
    });
    resetTabSessionId();
    return res.json();
  },

  async getUsers(token) {
    const res = await fetchWithFailover(`${API_BASE}/api/users?token=${encodeURIComponent(token)}`);
    const data = await res.json();
    if (data && data.success && Array.isArray(data.users)) {
      data.users = data.users.filter(u => {
        const name = (u.username || "").toLowerCase();
        return !name.startsWith("check_") && !name.startsWith("monitor_") && !name.startsWith("test_user_");
      });
    }
    return data;
  },

  async getGroups(token) {
    const res = await fetchWithFailover(`${API_BASE}/api/groups?token=${encodeURIComponent(token)}`);
    return res.json();
  },

  eventStreamUrl(token) {
    const sess = getTabSessionId();
    return `${API_BASE}/api/events?token=${encodeURIComponent(token)}&session_id=${encodeURIComponent(sess)}`;
  },

  async getDMs(token, otherUserId, signal) {
    const res = await fetchWithFailover(
      `${API_BASE}/api/messages/dm?token=${encodeURIComponent(token)}&other_user_id=${encodeURIComponent(otherUserId)}`,
      { signal }
    );
    return res.json();
  },

  async sendDM(token, recipientUserId, content, fileId) {
    const reqId = "user-" + (typeof crypto !== "undefined" && crypto.randomUUID ? crypto.randomUUID() : (Date.now().toString(36) + Math.random().toString(36).slice(2)));
    const res = await fetchWithFailover(`${API_BASE}/api/messages/dm`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        recipient_user_id: recipientUserId,
        content: content || "",
        client_request_id: reqId,
        file_id: fileId || null,
      }),
    });
    return res.json();
  },

  async getGroupMessages(token, groupId, signal) {
    const res = await fetchWithFailover(
      `${API_BASE}/api/messages/group?token=${encodeURIComponent(token)}&group_id=${encodeURIComponent(groupId)}`,
      { signal }
    );
    return res.json();
  },

  async sendGroupMessage(token, groupId, content, fileId) {
    const reqId = "user-" + (typeof crypto !== "undefined" && crypto.randomUUID ? crypto.randomUUID() : (Date.now().toString(36) + Math.random().toString(36).slice(2)));
    const res = await fetchWithFailover(`${API_BASE}/api/messages/group`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        group_id: groupId,
        content: content || "",
        client_request_id: reqId,
        file_id: fileId || null,
      }),
    });
    return res.json();
  },

  async createGroup(token, name, initialMemberUserIds) {
    const res = await fetchWithFailover(`${API_BASE}/api/groups`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        name,
        initial_member_user_ids: initialMemberUserIds,
      }),
    });
    return res.json();
  },

  async updateGroup(token, groupId, action, targetUserId, newName) {
    const res = await fetchWithFailover(`${API_BASE}/api/groups/update`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        group_id: groupId,
        action,
        target_user_id: targetUserId,
        new_name: newName,
      }),
    });
    return res.json();
  },

  async getGroupMembers(token, groupId) {
    const res = await fetchWithFailover(
      `${API_BASE}/api/groups/members?token=${encodeURIComponent(token)}&group_id=${encodeURIComponent(groupId)}`
    );
    return res.json();
  },

  async uploadFile(token, chatType, targetId, file) {
    const formData = new FormData();
    formData.append("token", token);
    formData.append("chat_type", chatType);
    formData.append("target_id", targetId);
    formData.append("file", file);

    const res = await fetchWithFailover(`${API_BASE}/api/files/upload?token=${encodeURIComponent(token)}`, {
      method: "POST",
      body: formData,
    });
    return res.json();
  },

  async smartReplies(token, chatType, targetId, chatHistory, currentMessage, contextTitle) {
    const res = await fetchWithFailover(`${API_BASE}/api/llm/smart-reply`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        chat_type: chatType,
        target_id: targetId,
        chat_history: chatHistory,
        current_message: currentMessage,
        context_title: contextTitle,
        request_id: crypto.randomUUID(),
      }),
    });
    return res.json();
  },

  async summarize(token, chatType, targetId, chatHistory, contextTitle, currentUser) {
    const res = await fetchWithFailover(`${API_BASE}/api/llm/summarize`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        chat_type: chatType,
        target_id: targetId,
        chat_history: chatHistory,
        context_title: contextTitle,
        current_user: currentUser,
        request_id: crypto.randomUUID(),
      }),
    });
    return res.json();
  },

  async suggest(token, chatType, targetId, chatHistory, contextTitle) {
    const res = await fetchWithFailover(`${API_BASE}/api/llm/suggest`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        chat_type: chatType,
        target_id: targetId,
        chat_history: chatHistory,
        context_title: contextTitle,
        request_id: crypto.randomUUID(),
      }),
    });
    return res.json();
  },

  async markRead(token, chatType, targetId) {
    const res = await fetchWithFailover(`${API_BASE}/api/chat/read`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        chat_type: chatType,
        target_id: targetId,
      }),
    });
    return res.json();
  },
};
