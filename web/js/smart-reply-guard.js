function isSmartReplyRequestCurrent(request, state) {
  return Boolean(
    request && state &&
    request.token && request.token === state.token &&
    request.chatKey && request.chatKey === state.chatKey &&
    request.messageId && request.messageId === state.messageId
  );
}

function hasSmartReplyText(message) {
  const content = String(message?.content || "").trim();
  if (!content) return false;
  return !(message.file_id && content.toLowerCase() === "[file attachment]");
}

if (typeof window !== "undefined") {
  window.isSmartReplyRequestCurrent = isSmartReplyRequestCurrent;
  window.hasSmartReplyText = hasSmartReplyText;
}

if (typeof module !== "undefined" && module.exports) {
  module.exports = { isSmartReplyRequestCurrent, hasSmartReplyText };
}
