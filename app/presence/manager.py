"""
Presence manager.
Users who haven't sent a heartbeat in OFFLINE_THRESHOLD seconds are marked OFFLINE.
A background thread performs this sweep every SWEEP_INTERVAL seconds.
"""

import logging
import threading
import time
from typing import List

logger = logging.getLogger(__name__)

OFFLINE_THRESHOLD = 60   # seconds without heartbeat → OFFLINE
SWEEP_INTERVAL    = 30   # seconds between sweeps


class PresenceManager:
    def __init__(self, db):
        self.db       = db
        self._running = True
        self._sweeper = threading.Thread(
            target=self._sweep_loop, daemon=True, name="presence-sweeper"
        )
        self._sweeper.start()

    def update_presence(self, user_id: str, status: str = "ONLINE"):
        now = int(time.time())
        self.db.execute(
            "INSERT OR REPLACE INTO presence (user_id, status, last_seen) VALUES (?, ?, ?)",
            (user_id, status, now),
        )
        self.db.commit()

    def get_all_presence(self) -> List[dict]:
        rows = self.db.fetchall(
            """
            SELECT p.user_id, u.username, p.status, p.last_seen
            FROM presence p
            JOIN users u ON p.user_id = u.user_id
            """
        )
        return [dict(r) for r in rows]

    def get_channel_presence(self, channel_id: str) -> List[dict]:
        rows = self.db.fetchall(
            """
            SELECT p.user_id, u.username, p.status, p.last_seen
            FROM presence p
            JOIN users u ON p.user_id = u.user_id
            JOIN channel_members cm ON cm.user_id = p.user_id
            WHERE cm.channel_id = ?
            """,
            (channel_id,),
        )
        return [dict(r) for r in rows]

    def _sweep_loop(self):
        while self._running:
            time.sleep(SWEEP_INTERVAL)
            try:
                threshold = int(time.time()) - OFFLINE_THRESHOLD
                self.db.execute(
                    "UPDATE presence SET status = 'OFFLINE'"
                    " WHERE status = 'ONLINE' AND last_seen < ?",
                    (threshold,),
                )
                self.db.commit()
            except Exception as exc:
                logger.error("[PRESENCE] Sweep error: %s", exc)

    def stop(self):
        self._running = False

