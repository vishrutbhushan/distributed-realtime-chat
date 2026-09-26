"""Focused unit tests for the M1 persistence and manager behavior."""

import os
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from app.auth.manager import AuthManager
from app.chat.manager import ChatManager
from app.files.manager import FileManager
from app.presence.manager import PresenceManager
from storage.database import Database


class ManagerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "chat.db")
        self.db = Database(self.db_path)
        self.auth = AuthManager(self.db)
        self.chat = ChatManager(self.db)
        self.files = FileManager(self.db, os.path.join(self.temp_dir.name, "files"))
        ok, self.user_id, message = self.auth.create_user("tester", "pw", "USER")
        self.assertTrue(ok, message)
        ok, self.channel, message = self.chat.create_channel("test-room", self.user_id)
        self.assertTrue(ok, message)

    def tearDown(self):
        self.db.close()
        self.temp_dir.cleanup()

    def test_transaction_rolls_back_and_database_remains_writable(self):
        with self.assertRaisesRegex(RuntimeError, "force rollback"):
            with self.db.transaction(immediate=True):
                self.db.execute(
                    "UPDATE channels SET name = ? WHERE channel_id = ?",
                    ("should-not-stick", self.channel["channel_id"]),
                )
                raise RuntimeError("force rollback")
        self.assertEqual(self.chat.get_channel(self.channel["channel_id"])["name"], "test-room")
        ok, channel, message = self.chat.create_channel("still-writable", self.user_id)
        self.assertTrue(ok, message)
        self.assertEqual(channel["name"], "still-writable")

    def test_duplicate_channel_failure_does_not_leave_open_transaction(self):
        ok, _, message = self.chat.create_channel("test-room", self.user_id)
        self.assertFalse(ok)
        self.assertIn("already exists", message)
        ok, new_user_id, message = self.auth.create_user("after-duplicate", "pw")
        self.assertTrue(ok, message)
        self.assertTrue(new_user_id)

    def test_membership_is_required_and_history_order_is_stable_for_equal_timestamps(self):
        ok, outsider_id, message = self.auth.create_user("outsider", "pw")
        self.assertTrue(ok, message)
        sent, _, error = self.chat.send_message(
            self.channel["channel_id"], outsider_id, "denied"
        )
        self.assertFalse(sent)
        self.assertIn("Join the channel", error)

        expected = ["first", "second", "third"]
        for content in expected:
            sent, _, error = self.chat.send_message(
                self.channel["channel_id"], self.user_id, content
            )
            self.assertTrue(sent, error)
        with self.db.transaction(immediate=True):
            self.db.execute(
                "UPDATE messages SET timestamp = 123456 WHERE channel_id = ?",
                (self.channel["channel_id"],),
            )
        actual = [m["content"] for m in self.chat.get_messages(self.channel["channel_id"])]
        self.assertEqual(actual, expected)

    def test_concurrent_duplicate_retries_create_one_message(self):
        workers = 10
        barrier = threading.Barrier(workers)
        request_id = "same-request-id"

        def send_retry(_index):
            barrier.wait(timeout=5)
            return self.chat.send_message(
                self.channel["channel_id"],
                self.user_id,
                "one durable message",
                client_request_id=request_id,
            )

        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(send_retry, range(workers)))
        self.assertTrue(all(result[0] for result in results), [r[2] for r in results])
        ids = {result[1]["message_id"] for result in results}
        self.assertEqual(len(ids), 1)
        self.assertEqual(len(self.chat.get_messages(self.channel["channel_id"])), 1)

    def test_multi_session_logout_presence_expiry_and_user_removal(self):
        first = self.auth.login("tester", "pw")
        second = self.auth.login("tester", "pw")
        self.assertTrue(first[0] and second[0])
        self.assertTrue(self.auth.logout(first[1]))
        presence = self.db.fetchone("SELECT status FROM presence WHERE user_id = ?", (self.user_id,))
        self.assertEqual(presence["status"], "ONLINE")

        with self.db.transaction(immediate=True):
            self.db.execute("UPDATE sessions SET expires_at = 0 WHERE token = ?", (second[1],))
        self.assertIsNone(self.auth.validate_token(second[1]))
        self.assertTrue(self.auth.logout(second[1]))
        presence = self.db.fetchone("SELECT status FROM presence WHERE user_id = ?", (self.user_id,))
        self.assertEqual(presence["status"], "OFFLINE")

        sent, message, error = self.chat.send_message(
            self.channel["channel_id"], self.user_id, "historical record"
        )
        self.assertTrue(sent, error)
        removed, note = self.auth.remove_user(self.user_id)
        self.assertTrue(removed, note)
        self.assertFalse(self.auth.login("tester", "pw")[0])
        self.assertEqual(
            self.db.fetchone("SELECT content FROM messages WHERE message_id = ?", (message["message_id"],))["content"],
            "historical record",
        )
        self.assertEqual(self.chat.get_messages(self.channel["channel_id"])[0]["content"], "historical record")

    def test_file_round_trip_and_channel_delete_removes_metadata_and_blob(self):
        data = b"\x00M1 file round trip\xff"
        ok, meta, message = self.files.upload_file(
            self.channel["channel_id"], self.user_id, "sample.bin", data
        )
        self.assertTrue(ok, message)
        self.assertEqual(self.files.download_file(meta["file_id"])[1], data)
        self.assertTrue(os.path.isfile(meta["storage_location"]))

        deleted, message = self.chat.delete_channel(self.channel["channel_id"])
        self.assertTrue(deleted, message)
        self.assertFalse(os.path.exists(meta["storage_location"]))
        self.assertIsNone(self.files.get_file(meta["file_id"]))

    def test_presence_status_validation_and_channel_scope(self):
        presence = PresenceManager(self.db)
        try:
            presence.update_presence(self.user_id, "ONLINE")
            self.assertEqual(
                [u["username"] for u in presence.get_channel_presence(self.channel["channel_id"])],
                ["tester"],
            )
            with self.assertRaisesRegex(ValueError, "Presence status"):
                presence.update_presence(self.user_id, "MAYBE")
            presence.update_presence(self.user_id, "OFFLINE")
            self.assertEqual(presence.get_channel_presence(self.channel["channel_id"])[0]["status"], "OFFLINE")
        finally:
            presence.stop()


class DatabaseMigrationTests(unittest.TestCase):
    def test_adds_active_flag_to_existing_users_table(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "legacy.db")
            conn = sqlite3.connect(path)
            conn.execute(
                "CREATE TABLE users (user_id TEXT PRIMARY KEY, username TEXT UNIQUE NOT NULL, "
                "password_hash TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'USER', created_at INTEGER NOT NULL)"
            )
            conn.execute(
                "INSERT INTO users VALUES ('legacy', 'legacy-user', 'hash', 'USER', 1)"
            )
            conn.commit()
            conn.close()
            db = Database(path)
            try:
                user = db.fetchone("SELECT active FROM users WHERE user_id = 'legacy'")
                self.assertEqual(user["active"], 1)
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
