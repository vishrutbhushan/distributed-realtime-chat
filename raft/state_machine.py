"""
Raft state machine (Milestone 2 stub).

In M1 operations are applied directly without going through Raft.
In M2 this class will receive committed log entries and apply them.
"""

import logging

logger = logging.getLogger(__name__)


class StateMachine:
    """
    Applies committed Raft log entries to application state.
    M1: no-op — commands are applied directly by the managers.
    M2: this becomes the single point of application of committed entries.
    """

    def __init__(self, chat_manager, auth_manager):
        self.chat = chat_manager
        self.auth = auth_manager
        self.last_applied = 0

    def apply(self, entry) -> dict:
        """
        Apply a single LogEntry to application state.
        Returns result dict (may be empty for side-effect-only ops).
        """
        cmd = entry.command_type
        payload = entry.payload
        logger.debug("[STATE_MACHINE] Applying index=%d cmd=%s", entry.index, cmd)

        if cmd == "SEND_MESSAGE":
            ok, msg, err = self.chat.send_message(
                channel_id=payload["channel_id"],
                sender_id=payload["sender_id"],
                content=payload["content"],
                client_request_id=payload.get("client_request_id"),
                file_id=payload.get("file_id"),
            )
            self.last_applied = entry.index
            return {"success": ok, "message": msg, "error": err}

        elif cmd == "CREATE_CHANNEL":
            ok, ch, err = self.chat.create_channel(
                name=payload["name"],
                created_by=payload["created_by"],
            )
            self.last_applied = entry.index
            return {"success": ok, "channel": ch, "error": err}

        elif cmd == "JOIN_CHANNEL":
            ok, msg = self.chat.join_channel(
                channel_id=payload["channel_id"],
                user_id=payload["user_id"],
            )
            self.last_applied = entry.index
            return {"success": ok, "message": msg}

        else:
            logger.warning("[STATE_MACHINE] Unknown command: %s", cmd)
            self.last_applied = entry.index
            return {}
