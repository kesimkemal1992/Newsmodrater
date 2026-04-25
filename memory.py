"""
memory.py — Persistent memory, anti-duplication, and scheduler state engine.

Stores:
  • SHA-256 hashes of every processed message (text + image)
  • Posted message metadata for analytics
  • Channel state (last-seen message IDs)
  • Daily briefing post IDs (for reminder reply threading)
  • Daily reminder count (anti-spam: max 2 per day)
  • Reminder schedule state
"""

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
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

-- Tracks the morning daily briefing post (so reminders can reply to it)
CREATE TABLE IF NOT EXISTS daily_briefings (
    date_str        TEXT PRIMARY KEY,   -- YYYY-MM-DD in EAT timezone
    dest_msg_id     INTEGER NOT NULL,   -- Telegram message ID to reply to
    posted_at       REAL NOT NULL,
    events_json     TEXT                -- serialised list of events
);

-- Tracks how many reminders have been sent today (max 2 per day)
CREATE TABLE IF NOT EXISTS daily_reminders (
    date_str        TEXT PRIMARY KEY,   -- YYYY-MM-DD in EAT timezone
    count           INTEGER DEFAULT 0,
    last_sent       REAL
);

-- Tracks which events have already had their reminder sent
CREATE TABLE IF NOT EXISTS sent_reminders (
    event_key       TEXT PRIMARY KEY,   -- "{date_str}_{event_name}_{currency}"
    sent_at         REAL NOT NULL
);

-- Tracks the global motivational line index (cycles through the pool)
CREATE TABLE IF NOT EXISTS motivational_state (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    line_index  INTEGER DEFAULT 0,
    updated_at  REAL
);
INSERT OR IGNORE INTO motivational_state (id, line_index, updated_at) VALUES (1, 0, 0);

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

    # ── Daily Briefing Tracking ────────────────────────────────────────────────
    async def save_daily_briefing(self, date_str: str, dest_msg_id: int, events: list):
        """Store the morning briefing message ID so reminders can reply to it."""
        async with self._lock:
            await self._db.execute(
                """INSERT OR REPLACE INTO daily_briefings
                   (date_str, dest_msg_id, posted_at, events_json)
                   VALUES (?, ?, ?, ?)""",
                (date_str, dest_msg_id, time.time(), json.dumps(events, ensure_ascii=False)),
            )
            await self._db.commit()
        log.info(f"Daily briefing saved: date={date_str}, msg_id={dest_msg_id}")

    async def get_daily_briefing_msg_id(self, date_str: str) -> Optional[int]:
        """Return the Telegram message ID of today's briefing post (for reply threading)."""
        async with self._db.execute(
            "SELECT dest_msg_id FROM daily_briefings WHERE date_str=?", (date_str,)
        ) as cur:
            row = await cur.fetchone()
        return row["dest_msg_id"] if row else None

    async def has_daily_briefing(self, date_str: str) -> bool:
        """Check if morning briefing was already posted today."""
        msg_id = await self.get_daily_briefing_msg_id(date_str)
        return msg_id is not None

    # ── Reminder Anti-Spam ────────────────────────────────────────────────────
    async def get_reminder_count_today(self, date_str: str) -> int:
        """Return how many reminders have been sent today."""
        async with self._db.execute(
            "SELECT count FROM daily_reminders WHERE date_str=?", (date_str,)
        ) as cur:
            row = await cur.fetchone()
        return row["count"] if row else 0

    async def increment_reminder_count(self, date_str: str):
        """Increment the reminder counter for today (called after each reminder sent)."""
        async with self._lock:
            await self._db.execute(
                """INSERT INTO daily_reminders (date_str, count, last_sent)
                   VALUES (?, 1, ?)
                   ON CONFLICT(date_str) DO UPDATE
                   SET count=count+1, last_sent=excluded.last_sent""",
                (date_str, time.time()),
            )
            await self._db.commit()

    async def has_reminder_been_sent(self, event_key: str) -> bool:
        """Check if a reminder for a specific event has already been sent."""
        async with self._db.execute(
            "SELECT 1 FROM sent_reminders WHERE event_key=?", (event_key,)
        ) as cur:
            row = await cur.fetchone()
        return row is not None

    async def mark_reminder_sent(self, event_key: str):
        """Mark that a reminder for this event has been sent."""
        async with self._lock:
            await self._db.execute(
                "INSERT OR IGNORE INTO sent_reminders (event_key, sent_at) VALUES (?, ?)",
                (event_key, time.time()),
            )
            await self._db.commit()

    # ── Motivational Index ─────────────────────────────────────────────────────
    async def get_and_increment_motivational_index(self) -> int:
        """
        Return current motivational line index, then increment it.
        Cycles through pool of 20 lines. Persists across restarts.
        """
        async with self._lock:
            async with self._db.execute(
                "SELECT line_index FROM motivational_state WHERE id=1"
            ) as cur:
                row = await cur.fetchone()
            current = row["line_index"] if row else 0
            next_index = (current + 1) % 20  # pool size = 20
            await self._db.execute(
                "UPDATE motivational_state SET line_index=?, updated_at=? WHERE id=1",
                (next_index, __import__("time").time()),
            )
            await self._db.commit()
        return current

    # ── Eviction ───────────────────────────────────────────────────────────────
    async def _evict_expired(self):
        now = time.time()
        async with self._lock:
            cur = await self._db.execute(
                "DELETE FROM content_hashes WHERE expires_at<=?", (now,)
            )
            # Also clean up old reminder records (older than 7 days)
            old_threshold = now - (7 * 86_400)
            await self._db.execute(
                "DELETE FROM sent_reminders WHERE sent_at<?", (old_threshold,)
            )
            await self._db.commit()
        if cur.rowcount:
            log.info(f"Evicted {cur.rowcount} expired content hashes.")

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

        # Get today's reminder count
        from datetime import datetime, timezone
        import pytz
        eat = pytz.timezone("Africa/Addis_Ababa")
        today_str = datetime.now(eat).strftime("%Y-%m-%d")
        pending_reminders = await self.get_reminder_count_today(today_str)

        return {
            "tracked_hashes": hashes,
            "total_posted": posted,
            "posted_last_24h": posted_24h,
            "pending_reminders": pending_reminders,
        }
