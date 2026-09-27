"""File RPC authorization follows current DM and group membership."""

import os
import tempfile
import unittest

import chat_pb2

from app.auth.manager import AuthManager
from app.chat.manager import ChatManager
from app.files.manager import FileManager
from app.grpc_server import ChatServicer
from app.presence.manager import PresenceManager
from storage.database import Database


class FileAuthorizationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(os.path.join(self.temp_dir.name, "files.db"))
        self.chat = ChatManager(self.db)
        self.presence = PresenceManager(self.db, start_worker=False)
        self.service = ChatServicer.__new__(ChatServicer)
        self.service.db = self.db
        self.service.presence = self.presence
        self.service.auth = AuthManager(self.db, presence=self.presence)
        self.service.chat = self.chat
        self.service.files = FileManager(self.db, os.path.join(self.temp_dir.name, "uploads"))

    def tearDown(self):
        self.presence.stop()
        self.db.close()
        self.temp_dir.cleanup()

    def create_user(self, username):
        ok, token, user_id, _, error = self.service.auth.signup(username, "password")
        self.assertTrue(ok, error)
        return token, user_id

    def upload(self, token, chat_type, target_id, data=b"private bytes"):
        return self.service.UploadFile(
            chat_pb2.UploadFileRequest(
                token=token,
                chat_type=chat_type,
                target_id=target_id,
                filename="private.txt",
                data=data,
                content_type="text/plain",
            ),
            None,
        )

    def download(self, token, file_id):
        return self.service.DownloadFile(
            chat_pb2.DownloadFileRequest(token=token, file_id=file_id), None
        )

    def test_dm_file_is_available_only_to_its_two_participants(self):
        owner_token, owner_id = self.create_user("file_owner")
        recipient_token, recipient_id = self.create_user("file_recipient")
        outsider_token, _ = self.create_user("file_outsider")
        expected = b"DM file contents\x00\xff"

        uploaded = self.upload(owner_token, "DM", recipient_id, expected)
        self.assertTrue(uploaded.success, uploaded.message)
        self.assertEqual(uploaded.file.chat_type, "DM")

        recipient_file = self.download(recipient_token, uploaded.file.file_id)
        owner_file = self.download(owner_token, uploaded.file.file_id)
        outsider_file = self.download(outsider_token, uploaded.file.file_id)
        self.assertTrue(recipient_file.success)
        self.assertEqual(recipient_file.data, expected)
        self.assertTrue(owner_file.success)
        self.assertFalse(outsider_file.success)

    def test_group_file_requires_current_membership_for_upload_and_download(self):
        admin_token, admin_id = self.create_user("file_admin")
        member_token, member_id = self.create_user("file_member")
        outsider_token, outsider_id = self.create_user("file_outsider2")
        ok, group, error = self.service.chat.create_group("Files", admin_id, [member_id])
        self.assertTrue(ok, error)
        expected = b"Group file contents"

        uploaded = self.upload(admin_token, "GROUP", group["group_id"], expected)
        self.assertTrue(uploaded.success, uploaded.message)
        self.assertFalse(self.upload(outsider_token, "GROUP", group["group_id"]).success)
        member_file = self.download(member_token, uploaded.file.file_id)
        self.assertTrue(member_file.success)
        self.assertEqual(member_file.data, expected)
        self.assertFalse(self.download(outsider_token, uploaded.file.file_id).success)

        removed, _, error = self.service.chat.update_group(
            group_id=group["group_id"],
            requesting_user_id=admin_id,
            action="REMOVE_MEMBER",
            target_user_id=member_id,
        )
        self.assertTrue(removed, error)
        self.assertFalse(self.download(member_token, uploaded.file.file_id).success)

    def test_upload_rejects_unknown_dm_user_and_unsupported_chat_type(self):
        token, _ = self.create_user("file_sender")
        unknown_user = self.upload(token, "DM", "not-a-user")
        unsupported = self.upload(token, "OTHER", "anything")
        self.assertFalse(unknown_user.success)
        self.assertIn("not found", unknown_user.message.lower())
        self.assertFalse(unsupported.success)
        self.assertIn("unsupported", unsupported.message.lower())


if __name__ == "__main__":
    unittest.main()
