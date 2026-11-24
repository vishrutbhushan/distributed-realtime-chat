"""Bounded, in-memory Raft observations for the monitoring container."""

from collections import deque
from threading import Lock
import time
from uuid import uuid4


class RaftTelemetry:
    """Ring-buffer event recorder exposed to the Raft Observatory dashboard (:8088)."""
    def __init__(self):
        # Unique boot ID detects container restarts; bounded deque keeps last 512 events
        self.boot = uuid4().hex
        self._events = deque(maxlen=512)
        self._sequence = 0
        self._lock = Lock()

    def emit(self, kind, term, **fields):
        """Record a Raft event (election, vote, append, commit, apply) with monotonic seq."""
        with self._lock:
            self._sequence += 1
            self._events.append({
                "seq": self._sequence, "time": time.time(), "kind": kind,
                "term": term, **fields,
            })

    def read(self, after, boot):
        """Return incremental events with seq > after (resets cursor if node rebooted)."""
        with self._lock:
            if boot != self.boot:
                after = 0
            oldest = self._events[0]["seq"] if self._events else 1
            return {
                "boot": self.boot,
                "gap": after > 0 and after < oldest - 1,
                "events": [event for event in self._events if event["seq"] > after],
            }
