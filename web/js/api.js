/**
 * HTTP REST API Client for the Distributed Real-time Chat Gateway
 */

const API_BASE = "";

const api = {
  async signup(username, password) {
    const res = await fetch(`${API_BASE}/api/signup`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
    return res.json();
  },

  async login(username, password) {
    const res = await fetch(`${API_BASE}/api/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
    return res.json();
  },

  async logout(token) {
    const res = await fetch(`${API_BASE}/api/logout`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token }),
    });
    return res.json();
  },

  async getUsers(token) {
    const res = await fetch(`${API_BASE}/api/users?token=${encodeURIComponent(token)}`);
    return res.json();
  },

  async getGroups(token) {
    const res = await fetch(`${API_BASE}/api/groups?token=${encodeURIComponent(token)}`);
    return res.json();
  },

  async getDMs(token, otherUserId) {
    const res = await fetch(
      `${API_BASE}/api/messages/dm?token=${encodeURIComponent(token)}&other_user_id=${encodeURIComponent(otherUserId)}`
    );
    return res.json();
  },

  async sendDM(token, recipientUserId, content, fileId) {
    const res = await fetch(`${API_BASE}/api/messages/dm`, {
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

  async getGroupMessages(token, groupId) {
    const res = await fetch(
      `${API_BASE}/api/messages/group?token=${encodeURIComponent(token)}&group_id=${encodeURIComponent(groupId)}`
    );
    return res.json();
  },

  async sendGroupMessage(token, groupId, content, fileId) {
    const res = await fetch(`${API_BASE}/api/messages/group`, {
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
    const res = await fetch(`${API_BASE}/api/groups`, {
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
    const res = await fetch(`${API_BASE}/api/groups/update`, {
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
    const res = await fetch(
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

    const res = await fetch(`${API_BASE}/api/files/upload`, {
      method: "POST",
      body: formData,
    });
    return res.json();
  },

  async smartReplies(token, chatType, targetId, chatHistory, currentMessage, contextTitle) {
    const res = await fetch(`${API_BASE}/api/llm/smart-reply`, {
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

  async summarize(token, chatType, targetId, chatHistory, contextTitle) {
    const res = await fetch(`${API_BASE}/api/llm/summarize`, {
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
};
