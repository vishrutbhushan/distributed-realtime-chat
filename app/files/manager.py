"""
File manager.
Files are written to local filesystem; metadata is stored in SQLite.
In Milestone 2, file metadata will be replicated through the Raft log.
"""

import logging
import os
import time
import uuid
from typing import List, Tuple

logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB safeguard


class FileManager:
    def __init__(self, db, storage_path: str):
        self.db           = db
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)

    def upload_file(
        self,
        channel_id: str,
        owner_id: str,
        filename: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        request_id: str = None,
    ) -> Tuple[bool, dict, str]:
        """Persist file and metadata. Returns (success, metadata_dict, message)."""
        filename = (filename or "").strip()
        if not filename or "\x00" in filename:
            return False, {}, "A valid filename is required"
        if not channel_id or not owner_id:
            return False, {}, "Channel and owner are required"
        if len(data) > MAX_FILE_SIZE:
            return False, {}, f"File too large (max {MAX_FILE_SIZE // 1024 // 1024} MB)"

        file_id          = str(uuid.uuid4())
        storage_location = os.path.join(self.storage_path, file_id)
        now              = int(time.time())

        try:
            with open(storage_location, "wb") as fh:
                fh.write(data)

            with self.db.transaction(immediate=True):
                if not self.db.fetchone(
                    "SELECT 1 FROM channels WHERE channel_id = ?", (channel_id,)
                ):
                    raise ValueError("Channel not found")
                if not self.db.fetchone(
                    "SELECT 1 FROM users WHERE user_id = ? AND active = 1", (owner_id,)
                ):
                    raise ValueError("File owner account is inactive or missing")
                self.db.execute(
                    """
                    INSERT INTO files
                        (file_id, filename, owner_id, channel_id,
                         storage_location, size_bytes, content_type, uploaded_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (file_id, filename, owner_id, channel_id,
                     storage_location, len(data), content_type, now),
                )

            meta = {
                "file_id":          file_id,
                "filename":         filename,
                "owner_id":         owner_id,
                "channel_id":       channel_id,
                "storage_location": storage_location,
                "size_bytes":       len(data),
                "content_type":     content_type,
                "uploaded_at":      now,
            }
            logger.info("[FILES] Uploaded %s (%d bytes)", filename, len(data))
            return True, meta, "File uploaded"
        except Exception as exc:
            self.db.rollback()
            # Clean up orphaned file
            if os.path.exists(storage_location):
                os.remove(storage_location)
            logger.error("[FILES] upload error: %s", exc)
            return False, {}, str(exc)

    def download_file(self, file_id: str) -> Tuple[bool, bytes, dict, str]:
        """Returns (success, data, metadata, message)."""
        row = self.db.fetchone("SELECT * FROM files WHERE file_id = ?", (file_id,))
        if not row:
            return False, b"", {}, "File not found"
        meta = dict(row)
        try:
            with open(meta["storage_location"], "rb") as fh:
                data = fh.read()
            return True, data, meta, "OK"
        except Exception as exc:
            logger.error("[FILES] download error: %s", exc)
            return False, b"", meta, str(exc)

    def list_files(self, channel_id: str) -> List[dict]:
        rows = self.db.fetchall(
            "SELECT * FROM files WHERE channel_id = ? ORDER BY uploaded_at, rowid",
            (channel_id,),
        )
        return [dict(r) for r in rows]

    def get_file(self, file_id: str):
        row = self.db.fetchone("SELECT * FROM files WHERE file_id = ?", (file_id,))
        return dict(row) if row else None

