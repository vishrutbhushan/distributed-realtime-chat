"""
Comprehensive unit tests for M1 managers:
Auth, Direct Messaging, Group Messaging, Idempotency, and File Storage.
"""

import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from app.auth.manager import AuthManager
from app.chat.manager import ChatManager
from app.files.manager import FileManager
from storage.database import Database


class ManagerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "chat.db")
        self.db = Database(self.db_path)
        self.auth = AuthManager(self.db)
        self.chat = ChatManager(self.db)
        self.files = FileManager(self.db, os.path.join(self.temp_dir.name, "files"))

    def tearDown(self):
        self.db.close()
        self.temp_dir.cleanup()

    # ── Auth & Presence Tests ─────────────────────────────────────────────────

    def test_signup_validation_and_duplicate_rejection(self):
        # Username too short
        ok, _, _, _, msg = self.auth.signup("ab", "password123")
        self.assertFalse(ok)
        self.assertIn("3-20", msg)

        # Password too short
        ok, _, _, _, msg = self.auth.signup("validuser", "123")
        self.assertFalse(ok)
        self.assertIn("at least 4", msg)

        # Successful signup
        ok, token1, u1_id, u1_name, msg = self.auth.signup("user_one", "secret123")
        self.assertTrue(ok)
        self.assertTrue(token1)
        self.assertEqual(u1_name, "user_one")

        # Duplicate username rejection
        ok, _, _, _, msg = self.auth.signup("user_one", "secret456")
        self.assertFalse(ok)
        self.assertIn("already exists", msg)

    def test_login_logout_and_active_status(self):
        self.auth.signup("alice", "pass1234")
        ok, token, user_id, uname, msg = self.auth.login("alice", "pass1234")
        self.assertTrue(ok)

        # Check status is active
        user = self.auth.get_user_by_id(user_id)
        self.assertEqual(user["status"], "active")

        # Logout sets status to inactive
        self.auth.logout(token)
        user = self.auth.get_user_by_id(user_id)
        self.assertEqual(user["status"], "inactive")

        # Validate token should now return None
        self.assertIsNone(self.auth.validate_token(token))

    # ── Direct Messaging & Idempotency Tests ──────────────────────────────────

    def test_direct_messaging_and_history(self):
        _, t1, u1, _, _ = self.auth.signup("sender_user", "pass123")
        _, t2, u2, _, _ = self.auth.signup("receiver_user", "pass123")

        ok, msg, err = self.chat.send_dm(u1, u2, "Hello recipient!")
        self.assertTrue(ok, err)
        self.assertEqual(msg["content"], "Hello recipient!")

        # Both users can retrieve the direct message thread
        history1 = self.chat.get_dm_history(u1, u2)
        history2 = self.chat.get_dm_history(u2, u1)
        self.assertEqual(len(history1), 1)
        self.assertEqual(len(history2), 1)
        self.assertEqual(history1[0]["content"], "Hello recipient!")

    def test_concurrent_duplicate_retries_create_one_message(self):
        _, _, u1, _, _ = self.auth.signup("user_alice", "pass123")
        _, _, u2, _, _ = self.auth.signup("user_bob", "pass123")

        workers = 10
        barrier = threading.Barrier(workers)
        request_id = "unique-client-send-id-1234"

        def send_retry(_idx):
            barrier.wait(timeout=5)
            return self.chat.send_dm(
                u1, u2, "This message must not be duplicated", client_request_id=request_id
            )

        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(send_retry, range(workers)))

        self.assertTrue(all(r[0] for r in results))
        msg_ids = {r[1]["message_id"] for r in results}
        self.assertEqual(len(msg_ids), 1)

        history = self.chat.get_dm_history(u1, u2)
        self.assertEqual(len(history), 1)

    # ── Group Management & Admin Control Tests ────────────────────────────────

    def test_group_creation_and_admin_permissions(self):
        _, _, u_admin, _, _ = self.auth.signup("group_admin", "pass123")
        _, _, u_member1, _, _ = self.auth.signup("member_one", "pass123")
        _, _, u_member2, _, _ = self.auth.signup("member_two", "pass123")

        # 1. Create group with initial members
        ok, grp, err = self.chat.create_group(
            "Project Alpha", u_admin, [u_member1]
        )
        self.assertTrue(ok, err)
        grp_id = grp["group_id"]

        ok, members, err = self.chat.get_group_members(grp_id, u_admin)
        self.assertTrue(ok)
        self.assertEqual(len(members), 2)
        roles = {m["user_id"]: m["role"] for m in members}
        self.assertEqual(roles[u_admin], "ADMIN")
        self.assertEqual(roles[u_member1], "MEMBER")

        # 2. Non-admin cannot rename group
        ok, _, err = self.chat.update_group(
            group_id=grp_id, requesting_user_id=u_member1, action="RENAME", new_name="Hacked Name"
        )
        self.assertFalse(ok)
        self.assertIn("Only group admin", err)

        # 3. Admin renames group and adds u_member2
        ok, updated_grp, err = self.chat.update_group(
            group_id=grp_id, requesting_user_id=u_admin, action="RENAME", new_name="Project Alpha Renamed"
        )
        self.assertTrue(ok, err)
        self.assertEqual(updated_grp["name"], "Project Alpha Renamed")

        ok, _, err = self.chat.update_group(
            group_id=grp_id, requesting_user_id=u_admin, action="ADD_MEMBER", target_user_id=u_member2
        )
        self.assertTrue(ok, err)
        ok, members, _ = self.chat.get_group_members(grp_id, u_admin)
        self.assertEqual(len(members), 3)

        # 4. Admin promotes u_member1 to ADMIN
        ok, _, err = self.chat.update_group(
            group_id=grp_id, requesting_user_id=u_admin, action="MAKE_ADMIN", target_user_id=u_member1
        )
        self.assertTrue(ok, err)
        ok, members, _ = self.chat.get_group_members(grp_id, u_admin)
        roles = {m["user_id"]: m["role"] for m in members}
        self.assertEqual(roles[u_member1], "ADMIN")

    # ── File Upload and Download Tests ────────────────────────────────────────

    def test_file_upload_and_download(self):
        _, _, u1, _, _ = self.auth.signup("file_owner", "pass123")
        sample_bytes = b"%PDF-1.4 sample pdf document binary content \x00\x01\x02"

        ok, meta, err = self.files.upload_file(
            owner_id=u1,
            chat_type="DM",
            target_id="recipient-id-dummy",
            filename="document.pdf",
            data=sample_bytes,
            content_type="application/pdf",
        )
        self.assertTrue(ok, err)
        self.assertEqual(meta["filename"], "document.pdf")
        self.assertEqual(meta["file_type"], "pdf")

        # Download back
        ok, downloaded_bytes, downloaded_meta, err = self.files.download_file(meta["file_id"])
        self.assertTrue(ok, err)
        self.assertEqual(downloaded_bytes, sample_bytes)
        self.assertEqual(downloaded_meta["filename"], "document.pdf")


if __name__ == "__main__":
    unittest.main()
