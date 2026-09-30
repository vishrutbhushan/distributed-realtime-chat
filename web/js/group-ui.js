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
        <label for="chk-${u.user_id}">
          <span class="select-user-name">${escapeHtml(u.username)}</span>
          <span class="select-user-status">${escapeHtml(u.status)}</span>
        </label>
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
      await loadDirectory();
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
      await loadDirectory();
      await openManageGroupModal();
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

