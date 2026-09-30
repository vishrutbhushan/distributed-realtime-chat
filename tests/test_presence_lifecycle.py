"""Session and event-stream presence lifecycle tests."""

import os
import tempfile
import time
import unittest

from app.auth.manager import AuthManager
from app.presence.manager import PresenceManager
from storage.database import Database


class FakeClock:
    def __init__(self):
        self.monotonic_value = 1000.0
        self.wall_value = time.time()

    def monotonic(self):
        return self.monotonic_value

    def wall(self):
        return self.wall_value

    def advance(self, seconds):
        self.monotonic_value += seconds
        self.wall_value += seconds


class PresenceLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(os.path.join(self.temp_dir.name, "presence.db"))
        self.clock = FakeClock()
        self.changed_users = []
        self.presence = PresenceManager(
            self.db,
            on_presence_change=lambda user_ids: self.changed_users.extend(user_ids),
            monotonic=self.clock.monotonic,
            wall_clock=self.clock.wall,
            start_worker=False,
        )
        self.auth = AuthManager(self.db, presence=self.presence)

    def tearDown(self):
        self.presence.stop()
        self.db.close()
        self.temp_dir.cleanup()

    def create_user(self, username="presence_user", client_kind="browser"):
        ok, token, user_id, _, error = self.auth.signup(
            username, "password123", client_kind=client_kind
        )
        self.assertTrue(ok, error)
        return token, user_id

    def status(self, user_id):
        return self.auth.get_user_by_id(user_id)["status"]

    def test_idle_browser_stream_stays_active_past_rpc_timeout(self):
        token, user_id = self.create_user()
        self.presence.connect_stream(user_id, token, "stream-a", int(self.clock.wall()) + 1000)

        self.clock.advance(301)
        self.presence.sweep_once()

        self.assertEqual(self.status(user_id), "active")

    def test_last_tab_close_goes_inactive_after_grace(self):
        token, user_id = self.create_user()
        self.presence.connect_stream(user_id, token, "stream-a", int(self.clock.wall()) + 1000)
        self.presence.disconnect_stream("stream-a")

        self.clock.advance(9.9)
        self.presence.sweep_once()
        self.assertEqual(self.status(user_id), "active")

        self.clock.advance(0.2)
        self.presence.sweep_once()
        self.assertEqual(self.status(user_id), "inactive")
        self.assertEqual(self.changed_users.count(user_id), 2)

    def test_reconnect_during_grace_cancels_offline_transition(self):
        token, user_id = self.create_user()
        expiry = int(self.clock.wall()) + 1000
        self.presence.connect_stream(user_id, token, "stream-old", expiry)
        self.presence.disconnect_stream("stream-old")

        self.clock.advance(9)
        self.presence.connect_stream(user_id, token, "stream-new", expiry)
        self.clock.advance(2)
        self.presence.sweep_once()

        self.assertEqual(self.status(user_id), "active")
        self.assertEqual(self.changed_users.count(user_id), 1)

    def test_one_of_two_tabs_sharing_a_token_keeps_presence_active(self):
        token, user_id = self.create_user()
        expiry = int(self.clock.wall()) + 1000
        self.presence.connect_stream(user_id, token, "tab-one", expiry)
        self.presence.connect_stream(user_id, token, "tab-two", expiry)

        self.presence.disconnect_stream("tab-one")
        self.clock.advance(11)
        self.presence.sweep_once()

        self.assertEqual(self.status(user_id), "active")

        self.presence.disconnect_stream("tab-two")
        self.clock.advance(11)
        self.presence.sweep_once()
        self.assertEqual(self.status(user_id), "inactive")

    def test_multiple_tabs_and_sessions_keep_user_active_until_last_logout(self):
        token_one, user_id = self.create_user()
        expiry = int(self.clock.wall()) + 1000
        self.presence.connect_stream(user_id, token_one, "tab-one", expiry)
        self.presence.connect_stream(user_id, token_one, "tab-two", expiry)

        ok, token_two, login_id, _, error = self.auth.login(
            "presence_user", "password123", client_kind="browser"
        )
        self.assertTrue(ok, error)
        self.assertEqual(login_id, user_id)
        self.presence.connect_stream(user_id, token_two, "tab-three", expiry)

        self.presence.disconnect_stream("tab-one")
        self.auth.logout(token_one)
        self.assertEqual(self.status(user_id), "active")

        self.presence.disconnect_stream("tab-two")
        self.presence.disconnect_stream("tab-three")
        self.auth.logout(token_two)
        self.assertEqual(self.status(user_id), "inactive")

    def test_logout_ignores_an_unused_unexpired_login_token(self):
        token, user_id = self.create_user()
        self.presence.connect_stream(user_id, token, "live-tab", int(self.clock.wall()) + 1000)
        self.presence.connect_stream(user_id, token, "second-live-tab", int(self.clock.wall()) + 1000)
        self.db.execute(
            "INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
            ("unused-old-token", user_id, int(self.clock.wall()), int(self.clock.wall()) + 1000),
        )
        self.db.commit()

        self.assertTrue(self.auth.logout(token))

        self.assertEqual(self.status(user_id), "inactive")

    def test_late_browser_snapshot_cannot_reactivate_closed_tab(self):
        token, user_id = self.create_user()
        expiry = int(self.clock.wall()) + 1000
        self.presence.connect_stream(user_id, token, "stream-a", expiry)
        self.presence.disconnect_stream("stream-a")
        self.clock.advance(11)
        self.presence.sweep_once()
        self.assertEqual(self.status(user_id), "inactive")

        self.presence.note_rpc_activity(user_id, token, expiry, "browser")
        self.presence.sweep_once()

        self.assertEqual(self.status(user_id), "inactive")

    def test_rpc_activity_keeps_five_minute_presence(self):
        token, user_id = self.create_user(client_kind="rpc")
        self.clock.advance(301)
        self.presence.sweep_once()
        self.assertEqual(self.status(user_id), "inactive")

        session = self.auth.validate_token(token)
        self.presence.note_rpc_activity(
            user_id, token, session["expires_at"], "rpc"
        )
        self.clock.advance(299)
        self.presence.sweep_once()
        self.assertEqual(self.status(user_id), "active")

        self.clock.advance(2)
        self.presence.sweep_once()
        self.assertEqual(self.status(user_id), "inactive")

    def test_expired_stream_session_is_removed_and_marked_inactive(self):
        token, user_id = self.create_user()
        expiry = self.auth.validate_token(token)["expires_at"]
        self.presence.connect_stream(user_id, token, "stream-a", expiry)
        self.clock.advance(expiry - int(self.clock.wall()) + 1)

        self.presence.sweep_once()

        self.assertEqual(self.status(user_id), "inactive")

    def test_startup_resets_stale_persisted_presence_without_deleting_users(self):
        _, user_id = self.create_user()
        self.presence.reset_stale_presence()

        self.assertEqual(self.status(user_id), "inactive")
        self.assertIsNotNone(self.auth.get_user_by_id(user_id))


if __name__ == "__main__":
    unittest.main()
