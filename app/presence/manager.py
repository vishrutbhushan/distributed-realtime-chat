"""
Presence manager: manages active/inactive status sweeper.

Users who haven't sent a heartbeat/request in OFFLINE_THRESHOLD seconds are marked 'inactive'.
A background thread sweeps every SWEEP_INTERVAL seconds.
"""

import logging
import threading
import time

logger = logging.getLogger(__name__)

OFFLINE_THRESHOLD = 300  # 5 minutes without activity -> inactive
SWEEP_INTERVAL    = 30   # sweep every 30 seconds


class PresenceManager:
    def __init__(self, db):
        self.db = db
        self._running = True
        self._sweeper = threading.Thread(
            target=self._sweep_loop, daemon=True, name="presence-sweeper"
        )
        self._sweeper.start()

    def update_presence(self, user_id: str, status: str = "active"):
        now = int(time.time())
        try:
            self.db.execute(
                "UPDATE users SET status = ?, last_seen = ? WHERE user_id = ?",
                (status, now, user_id),
            )
            self.db.commit()
        except Exception as exc:
            logger.error("[PRESENCE] update_presence error: %s", exc)

    def _sweep_loop(self):
        while self._running:
            time.sleep(SWEEP_INTERVAL)
            try:
                threshold = int(time.time()) - OFFLINE_THRESHOLD
                self.db.execute(
                    "UPDATE users SET status = 'inactive' WHERE status = 'active' AND last_seen < ?",
                    (threshold,),
                )
                self.db.commit()
            except Exception as exc:
                logger.error("[PRESENCE] Sweep error: %s", exc)

    def stop(self):
        self._running = False
