/**
 * HTTP REST API Client for the Distributed Real-time Chat Gateway
 */

const API_BASE = "";

function withPinnedNode(url) {
  try {
    const pinned = sessionStorage.getItem("chat_pinned_node");
    if (!pinned) return url;
    const sep = url.includes("?") ? "&" : "?";
    return `${url}${sep}node=${encodeURIComponent(pinned)}`;
  } catch (_) {
    return url;
  }
}

async function fetchWithFailover(url, options = {}, retries = 2) {
  for (let i = 0; i <= retries; i++) {
    try {
      const targetUrl = i === 0 ? withPinnedNode(url) : url;
      const res = await fetch(targetUrl, options);
      if (res.status >= 502 && i < retries) {
        try { sessionStorage.removeItem("chat_pinned_node"); } catch (_) { }
        await new Promise(r => setTimeout(r, 150));
        continue;
      }
      return res;
    } catch (err) {
      if (err.name === "AbortError" || i === retries) throw err;
      try { sessionStorage.removeItem("chat_pinned_node"); } catch (_) { }
      await new Promise(r => setTimeout(r, 150));
    }
  }
}

const api = {
  async getStatus() {
    const res = await fetchWithFailover(`${API_BASE}/api/status`);
    return res.json();
  },

  async signup(username, password) {
    const res = await fetchWithFailover(`${API_BASE}/api/signup`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
    return res.json();
  },

  async login(username, password) {
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
    return res.json();
  },

  async getUsers(token) {
    const res = await fetchWithFailover(`${API_BASE}/api/users?token=${encodeURIComponent(token)}`);
    return res.json();
  },

  async getGroups(token) {
    const res = await fetchWithFailover(`${API_BASE}/api/groups?token=${encodeURIComponent(token)}`);
    return res.json();
  },

  eventStreamUrl(token) {
    return withPinnedNode(`${API_BASE}/api/events?token=${encodeURIComponent(token)}`);
  },

  async getDMs(token, otherUserId, signal) {
    const res = await fetchWithFailover(
      `${API_BASE}/api/messages/dm?token=${encodeURIComponent(token)}&other_user_id=${encodeURIComponent(otherUserId)}`,
      { signal }
    );
    return res.json();
  },

  async sendDM(token, recipientUserId, content, fileId) {
    const res = await fetchWithFailover(`${API_BASE}/api/messages/dm`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        recipient_user_id: recipientUserId,
        content: content || "",
        client_request_id: crypto.randomUUID(),
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
    const res = await fetchWithFailover(`${API_BASE}/api/messages/group`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        group_id: groupId,
        content: content || "",
        client_request_id: crypto.randomUUID(),
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
