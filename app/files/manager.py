"""
File manager: handles PDF and Image upload/download.

Requirements:
- Send / receive PDF files and image files in DMs and groups.
- Persist files on disk and metadata in SQLite.
- File size safeguard (e.g. 50 MB).
- Content-type and file-type detection (image, pdf, other).
"""

import logging
import os
import time
import uuid
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB safeguard


class FileManager:
    def __init__(self, db, storage_path: str):
        self.db = db
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)

    def upload_file(
        self,
        owner_id: str,
        chat_type: str,
        target_id: str,
        filename: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        request_id: Optional[str] = None,
    ) -> Tuple[bool, dict, str]:
        """
        Persist binary file to filesystem and record metadata in SQLite.
        Detects whether file is 'image' or 'pdf'.
        """
        if len(data) > MAX_FILE_SIZE:
            return False, {}, f"File too large (max {MAX_FILE_SIZE // 1024 // 1024} MB)"

        filename = os.path.basename(filename or "file")
        lower_name = filename.lower()

        # Classify file_type
        if content_type.startswith("image/") or lower_name.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg")):
            file_type = "image"
        elif content_type == "application/pdf" or lower_name.endswith(".pdf"):
            file_type = "pdf"
        else:
            file_type = "other"

        file_id = str(uuid.uuid4())
        storage_location = os.path.join(self.storage_path, file_id)
        now = int(time.time())

        try:
            with open(storage_location, "wb") as fh:
                fh.write(data)

            self.db.execute(
                """
                INSERT INTO files
                    (file_id, filename, file_type, content_type, size_bytes,
                     owner_id, chat_type, target_id, storage_location, uploaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (file_id, filename, file_type, content_type, len(data),
                 owner_id, chat_type, target_id, storage_location, now),
            )
            self.db.commit()

            meta = {
                "file_id": file_id,
                "filename": filename,
                "file_type": file_type,
                "content_type": content_type,
                "size_bytes": len(data),
                "owner_id": owner_id,
                "chat_type": chat_type,
                "target_id": target_id,
                "storage_location": storage_location,
                "uploaded_at": now,
            }
            logger.info("[FILES] Uploaded file=%s type=%s size=%d", filename, file_type, len(data))
            return True, meta, "File uploaded successfully"
        except Exception as exc:
            self.db.rollback()
            if os.path.exists(storage_location):
                try:
                    os.remove(storage_location)
                except OSError:
                    pass
            logger.error("[FILES] upload error: %s", exc)
            return False, {}, str(exc)

    def download_file(self, file_id: str) -> Tuple[bool, bytes, dict, str]:
        """Fetch binary content and metadata for a file."""
        row = self.db.fetchone("SELECT * FROM files WHERE file_id = ?", (file_id,))
        if not row:
            return False, b"", {}, "File not found"

        meta = dict(row)
        loc = meta["storage_location"]
        if not os.path.exists(loc):
            return False, b"", meta, "File missing from disk storage"

        try:
            with open(loc, "rb") as fh:
                data = fh.read()
            return True, data, meta, "OK"
        except Exception as exc:
            logger.error("[FILES] download error: %s", exc)
            return False, b"", meta, str(exc)
