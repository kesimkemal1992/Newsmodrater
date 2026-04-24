"""
memory.py — Persistent memory & anti-duplication engine.

Stores:
  • SHA-256 hashes of every processed message (text + image)
  • Posted message metadata for self-learning analytics
  • Channel category embeddings (future: semantic dedup)
"""

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiosqlite

log = logging.getLogger("memory")

# ─── Schema ───────────────────────────────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS content_hashes (
    hash        TEXT PRIMARY KEY,
    source      TEXT,
    created_at  REAL NOT NULL,
    expires_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS posted_messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_channel  TEXT,
    source_msg_id   INTEGER,
    dest_msg_id     INTEGER,
    content_hash    TEXT,
    posted_at       REAL NOT NULL,
    ai_verdict      TEXT,
    formatted_text  TEXT
);

CREATE TABLE IF NOT EXISTS channel_stats (
    channel     TEXT PRIMARY KEY,
    last_msg_id INTEGER DEFAULT 0,
    last_seen   REAL
);

CREATE INDEX IF NOT EXISTS idx_hashes_expires ON content_hashes(expires_at);
CREATE INDEX IF NOT EXISTS idx_posted_source  ON posted_messages(source_channel, source_msg_id);
"""


class MemoryManager:
    def __init__(self, db_path: str = "memory.db", ttl_days: int = 30):
        self._db_path = db_path
        self._ttl_seconds = ttl_days * 86_400
        self._db: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    # ── Lifecycle ──────────────────────────────────────────────────────────────
    async def init(self):
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(_DDL)
        await self._db.commit()
        await self._evict_expired()
        log.info(f"Memory DB initialised at {self._db_path}")

    async def close(self):
        if self._db:
            await self._db.close()

    # ── Hashing helpers ────────────────────────────────────────────────────────
    @staticmethod
    def hash_text(text: str) -> str:
        """Normalise + hash text content."""
        normalised = " ".join(text.lower().split())
        return hashlib.sha256(normalised.encode()).hexdigest()

    @staticmethod
    def hash_bytes(data: bytes) -> str:
        """Hash raw bytes (image binary)."""
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def hash_combined(text: str, image_data: Optional[bytes]) -> str:
        """Hash text + optional image together."""
        h = hashlib.sha256()
        if text:
            h.update(" ".join(text.lower().split()).encode())
        if image_data:
            h.update(image_data)
        return h.hexdigest()

    # ── Deduplication ──────────────────────────────────────────────────────────
    async def is_duplicate(self, content_hash: str) -> bool:
        async with self._lock:
            now = time.time()
            async with self._db.execute(
                "SELECT 1 FROM content_hashes WHERE hash=? AND expires_at>?",
                (content_hash, now),
            ) as cur:
                row = await cur.fetchone()
            return row is not None

    async def mark_seen(self, content_hash: str, source: str = ""):
        now = time.time()
        expires = now + self._ttl_seconds
        async with self._lock:
            await self._db.execute(
                """INSERT OR REPLACE INTO content_hashes
                   (hash, source, created_at, expires_at)
                   VALUES (?, ?, ?, ?)""",
                (content_hash, source, now, expires),
            )
            await self._db.commit()

    # ── Posted log ─────────────────────────────────────────────────────────────
    async def log_posted(
        self,
        source_channel: str,
        source_msg_id: int,
        dest_msg_id: int,
        content_hash: str,
        ai_verdict: dict,
        formatted_text: str,
    ):
        async with self._lock:
            await self._db.execute(
                """INSERT INTO posted_messages
                   (source_channel, source_msg_id, dest_msg_id,
                    content_hash, posted_at, ai_verdict, formatted_text)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    source_channel,
                    source_msg_id,
                    dest_msg_id,
                    content_hash,
                    time.time(),
                    json.dumps(ai_verdict, ensure_ascii=False),
                    formatted_text,
                ),
            )
            await self._db.commit()

    # ── Channel state (last-seen message IDs) ──────────────────────────────────
    async def get_last_msg_id(self, channel: str) -> int:
        async with self._db.execute(
            "SELECT last_msg_id FROM channel_stats WHERE channel=?", (channel,)
        ) as cur:
            row = await cur.fetchone()
        return row["last_msg_id"] if row else 0

    async def set_last_msg_id(self, channel: str, msg_id: int):
        async with self._lock:
            await self._db.execute(
                """INSERT INTO channel_stats (channel, last_msg_id, last_seen)
                   VALUES (?, ?, ?)
                   ON CONFLICT(channel) DO UPDATE
                   SET last_msg_id=excluded.last_msg_id,
                       last_seen=excluded.last_seen""",
                (channel, msg_id, time.time()),
            )
            await self._db.commit()

    # ── Eviction ───────────────────────────────────────────────────────────────
    async def _evict_expired(self):
        now = time.time()
        async with self._lock:
            cur = await self._db.execute(
                "DELETE FROM content_hashes WHERE expires_at<=?", (now,)
            )
            await self._db.commit()
        if cur.rowcount:
            log.info(f"Evicted {cur.rowcount} expired content hashes.")

    # ── Stats (for self-learning / monitoring) ─────────────────────────────────
    async def stats(self) -> dict:
        async with self._db.execute(
            "SELECT COUNT(*) AS n FROM content_hashes"
        ) as cur:
            hashes = (await cur.fetchone())["n"]
        async with self._db.execute(
            "SELECT COUNT(*) AS n FROM posted_messages"
        ) as cur:
            posted = (await cur.fetchone())["n"]
        async with self._db.execute(
            """SELECT COUNT(*) AS n FROM posted_messages
               WHERE posted_at > ?""",
            (time.time() - 86_400,),
        ) as cur:
            posted_24h = (await cur.fetchone())["n"]
        return {
            "tracked_hashes": hashes,
            "total_posted": posted,
            "posted_last_24h": posted_24h,
        }
