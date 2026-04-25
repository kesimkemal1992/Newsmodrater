"""
memory.py — Persistent memory & anti-duplication engine.

Stores:
  • SHA-256 hashes of every processed message (text + image)
  • Posted message metadata for analytics
  • Channel category last-seen message IDs
  • Scheduled reminders (calendar events) with sent status
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

-- NEW: tracks scheduled reminders for High Impact calendar events
CREATE TABLE IF NOT EXISTS reminders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key       TEXT UNIQUE NOT NULL,   -- "<event_name>|<news_time_utc>" normalised
    event_name      TEXT NOT NULL,
    news_time_utc   TEXT NOT NULL,          -- "HH:MM UTC"
    dest_msg_id     INTEGER NOT NULL,       -- the calendar post to reply to
    reminder_sent   INTEGER DEFAULT 0,      -- 0 = pending, 1 = sent, 2 = skipped
    created_at      REAL NOT NULL,
    sent_at         REAL
);

CREATE INDEX IF NOT EXISTS idx_hashes_expires    ON content_hashes(expires_at);
CREATE INDEX IF NOT EXISTS idx_posted_source     ON posted_messages(source_channel, source_msg_id);
CREATE INDEX IF NOT EXISTS idx_reminders_pending ON reminders(reminder_sent, news_time_utc);
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
        normalised = " ".join(text.lower().split())
        return hashlib.sha256(normalised.encode()).hexdigest()

    @staticmethod
    def hash_bytes(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def hash_combined(text: str, image_data: Optional[bytes]) -> str:
        h = hashlib.sha256()
        if text:
            h.update(" ".join(text.lower().split()).encode())
        if image_data:
            h.update(image_data)
        return h.hexdigest()

    @staticmethod
    def make_event_key(event_name: str, news_time_utc: str) -> str:
        """Normalised dedup key for a reminder."""
        name = " ".join(event_name.lower().strip().split())
        time_ = news_time_utc.lower().replace(" utc", "").strip()
        return f"{name}|{time_}"

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

    # ── Channel state ──────────────────────────────────────────────────────────
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

    # ── Reminder management ────────────────────────────────────────────────────

    async def reminder_exists(self, event_key: str) -> bool:
        """True if this event already has a reminder scheduled or sent."""
        async with self._db.execute(
            "SELECT 1 FROM reminders WHERE event_key=?", (event_key,)
        ) as cur:
            row = await cur.fetchone()
        return row is not None

    async def schedule_reminder(
        self,
        event_name: str,
        news_time_utc: str,
        dest_msg_id: int,
    ) -> bool:
        """
        Insert a pending reminder. Returns True if inserted, False if duplicate.
        """
        key = self.make_event_key(event_name, news_time_utc)
        if await self.reminder_exists(key):
            log.info(f"[REMINDER] Already exists for key='{key}' — skipping.")
            return False

        async with self._lock:
            try:
                await self._db.execute(
                    """INSERT INTO reminders
                       (event_key, event_name, news_time_utc, dest_msg_id,
                        reminder_sent, created_at)
                       VALUES (?, ?, ?, ?, 0, ?)""",
                    (key, event_name, news_time_utc, dest_msg_id, time.time()),
                )
                await self._db.commit()
                log.info(
                    f"[REMINDER] Scheduled → event='{event_name}' "
                    f"time='{news_time_utc}' reply_to={dest_msg_id}"
                )
                return True
            except Exception as exc:
                log.error(f"[REMINDER] Insert failed: {exc}")
                return False

    async def get_pending_reminders(self) -> list[dict]:
        """Return all reminders not yet sent or skipped."""
        async with self._db.execute(
            """SELECT id, event_key, event_name, news_time_utc, dest_msg_id
               FROM reminders
               WHERE reminder_sent = 0
               ORDER BY news_time_utc ASC""",
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def mark_reminder_sent(self, reminder_id: int, status: int = 1):
        """status: 1=sent, 2=skipped (too late / past)."""
        async with self._lock:
            await self._db.execute(
                "UPDATE reminders SET reminder_sent=?, sent_at=? WHERE id=?",
                (status, time.time(), reminder_id),
            )
            await self._db.commit()

    # ── Eviction ───────────────────────────────────────────────────────────────
    async def _evict_expired(self):
        now = time.time()
        async with self._lock:
            result = await self._db.execute(
                "DELETE FROM content_hashes WHERE expires_at<=?", (now,)
            )
            count = result.rowcount
            await self._db.commit()
        if count:
            log.info(f"Evicted {count} expired content hashes.")

    # ── Stats ──────────────────────────────────────────────────────────────────
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
            "SELECT COUNT(*) AS n FROM posted_messages WHERE posted_at > ?",
            (time.time() - 86_400,),
        ) as cur:
            posted_24h = (await cur.fetchone())["n"]
        async with self._db.execute(
            "SELECT COUNT(*) AS n FROM reminders WHERE reminder_sent = 0"
        ) as cur:
            pending_reminders = (await cur.fetchone())["n"]
        return {
            "tracked_hashes":    hashes,
            "total_posted":      posted,
            "posted_last_24h":   posted_24h,
            "pending_reminders": pending_reminders,
        }
