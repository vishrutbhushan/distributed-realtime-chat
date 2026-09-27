const assert = require("node:assert/strict");
const test = require("node:test");
const { isSmartReplyRequestCurrent, hasSmartReplyText } = require("../web/js/smart-reply-guard.js");

const request = { token: "session-a", chatKey: "DM:bob", messageId: "message-12" };

test("reply is current for the same user, chat, and latest message", () => {
  assert.equal(isSmartReplyRequestCurrent(request, {
    token: "session-a",
    chatKey: "DM:bob",
    messageId: "message-12",
  }), true);
});

test("reply is stale after switching chat, receiving a newer message, or changing session", () => {
  assert.equal(isSmartReplyRequestCurrent(request, {
    token: "session-a",
    chatKey: "GROUP:project",
    messageId: "message-12",
  }), false);
  assert.equal(isSmartReplyRequestCurrent(request, {
    token: "session-a",
    chatKey: "DM:bob",
    messageId: "message-13",
  }), false);
  assert.equal(isSmartReplyRequestCurrent(request, {
    token: "session-b",
    chatKey: "DM:bob",
    messageId: "message-12",
  }), false);
});

test("file-only messages do not request suggestions, while captions do", () => {
  assert.equal(hasSmartReplyText({ file_id: "file-1", content: "[File Attachment]" }), false);
  assert.equal(hasSmartReplyText({ file_id: "file-1", content: "  " }), false);
  assert.equal(hasSmartReplyText({ file_id: "file-1", content: "Please review this image." }), true);
  assert.equal(hasSmartReplyText({ content: "A text-only message." }), true);
});
