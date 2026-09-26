"""Persistent-volume startup seeding regression test (runs in the app image)."""

import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "generated"))
sys.path.insert(0, ROOT)

try:
    from app.server import ChatServicer
    from storage.database import Database
except ImportError as import_error:  # Host-only runs may not have generated gRPC stubs.
    ChatServicer = None
    Database = None
    IMPORT_ERROR = str(import_error)
else:
    IMPORT_ERROR = ""


@unittest.skipIf(ChatServicer is None, f"app stubs unavailable: {IMPORT_ERROR}")
class SeedRestartTests(unittest.TestCase):
    def test_seeding_is_idempotent_on_the_same_database(self):
        class StandaloneRaft:
            pass

        with tempfile.TemporaryDirectory() as directory:
            db = Database(os.path.join(directory, "chat.db"))
            first = ChatServicer(db, StandaloneRaft())
            try:
                second = ChatServicer(db, StandaloneRaft())
                try:
                    users = db.fetchone("SELECT COUNT(*) AS count FROM users")["count"]
                    channels = db.fetchone("SELECT COUNT(*) AS count FROM channels")["count"]
                    self.assertEqual(users, 3)
                    self.assertEqual(channels, 3)
                    self.assertTrue(second.auth.login("admin", "admin123")[0])
                finally:
                    second.presence.stop()
            finally:
                first.presence.stop()
                db.close()


if __name__ == "__main__":
    unittest.main()
