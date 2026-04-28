"""
memory.py — SQLite-backed memory manager for AXIOM INTEL.

Tracks:
  • Content hashes — deduplication (hash + AI similarity)
  • Recent post texts — for AI similarity comparison (last 20)
  • Posted messages — full audit log
  • Daily briefings — one per day, stores msg_id + events JSON
  • Weekly calendar — one per week
  • Reminders — sent status + daily count
  • Motivational index — rotates across restarts
  • Last message IDs — per source channel
"""

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List

import aiosqlite

log = logging.getLogger("memory")


class MemoryManager:
    def __init__(self, db_path: str = "memory.db", ttl_days: int = 30):
        self._db_path = db_path
        self._ttl_days = ttl_days
        self._db: Optional[aiosqlite.Connection] = None

    async def init(self):
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._create_tables()
        await self._db.commit()
        await self._cleanup_old_hashes()
        log.info(f"✅  MemoryManager ready — db={self._db_path} | ttl={self._ttl_days}d")

    async def close(self):
        if self._db:
            await self._db.close()

    # ── Schema ────────────────────────────────────────────────────────────────
    async def _create_tables(self):
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS content_hashes (
                hash        TEXT PRIMARY KEY,
                source      TEXT,
                seen_at     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS recent_post_texts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source_text TEXT NOT NULL,
                post_text   TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS posted_messages (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                source_channel  TEXT NOT NULL,
                source_msg_id   INTEGER NOT NULL,
                dest_msg_id     INTEGER NOT NULL,
                content_hash    TEXT NOT NULL,
                engine          TEXT,
                confidence      REAL,
                formatted_text  TEXT,
                posted_at       TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daily_briefings (
                date_str    TEXT PRIMARY KEY,
                msg_id      INTEGER NOT NULL,
                events_json TEXT,
                posted_at   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS weekly_calendar (
                week_key    TEXT PRIMARY KEY,
                posted_at   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reminders (
                event_key   TEXT PRIMARY KEY,
                sent_at     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reminder_counts (
                date_str    TEXT PRIMARY KEY,
                count       INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS channel_offsets (
                channel     TEXT PRIMARY KEY,
                last_msg_id INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS kv_store (
                key         TEXT PRIMARY KEY,
                value       TEXT NOT NULL
            );
        """)

    # ── Hash deduplication ────────────────────────────────────────────────────
    @staticmethod
    def hash_combined(text: str, image_data: Optional[bytes]) -> str:
        h = hashlib.sha256()
        if text:
            h.update(text.encode("utf-8", errors="replace"))
        if image_data:
            h.update(image_data[:4096])
        return h.hexdigest()

    async def is_duplicate(self, content_hash: str) -> bool:
        async with self._db.execute(
            "SELECT 1 FROM content_hashes WHERE hash=?", (content_hash,)
        ) as cur:
            return await cur.fetchone() is not None

    async def mark_seen(self, content_hash: str, source: str = ""):
        now = _utcnow()
        await self._db.execute(
            "INSERT OR IGNORE INTO content_hashes (hash, source, seen_at) VALUES (?, ?, ?)",
            (content_hash, source, now),
        )
        await self._db.commit()

    async def _cleanup_old_hashes(self):
        cutoff = (datetime.now(timezone.utc) - timedelta(days=self._ttl_days)).isoformat()
        await self._db.execute(
            "DELETE FROM content_hashes WHERE seen_at < ?", (cutoff,)
        )
        # Keep only last 100 recent post texts
        await self._db.execute("""
            DELETE FROM recent_post_texts
            WHERE id NOT IN (
                SELECT id FROM recent_post_texts ORDER BY id DESC LIMIT 100
            )
        """)
        await self._db.commit()

    # ── Recent post texts (for AI similarity check) ───────────────────────────
    async def store_recent_post_text(self, source_text: str, post_text: str):
        """Store source text of a posted story for future similarity checks."""
        await self._db.execute(
            "INSERT INTO recent_post_texts (source_text, post_text, created_at) VALUES (?, ?, ?)",
            (source_text[:1000], post_text[:1000], _utcnow()),
        )
        # Keep only last 50 entries
        await self._db.execute("""
            DELETE FROM recent_post_texts
            WHERE id NOT IN (
                SELECT id FROM recent_post_texts ORDER BY id DESC LIMIT 50
            )
        """)
        await self._db.commit()

    async def get_recent_post_texts(self, limit: int = 20) -> List[str]:
        """Return source_text of last N posted stories for similarity comparison."""
        async with self._db.execute(
            "SELECT source_text FROM recent_post_texts ORDER BY id DESC LIMIT ?", (limit,)
        ) as cur:
            rows = await cur.fetchall()
        return [row["source_text"] for row in rows]

    # ── Posted messages log ───────────────────────────────────────────────────
    async def log_posted(
        self,
        source_channel: str,
        source_msg_id: int,
        dest_msg_id: int,
        content_hash: str,
        ai_verdict: dict,
        formatted_text: str,
    ):
        await self._db.execute(
            """INSERT INTO posted_messages
               (source_channel, source_msg_id, dest_msg_id, content_hash,
                engine, confidence, formatted_text, posted_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                source_channel, source_msg_id, dest_msg_id, content_hash,
                ai_verdict.get("engine", ""), ai_verdict.get("confidence", 0.0),
                formatted_text, _utcnow(),
            ),
        )
        await self._db.commit()

    # ── Daily briefing ────────────────────────────────────────────────────────
    async def has_daily_briefing(self, date_str: str) -> bool:
        async with self._db.execute(
            "SELECT 1 FROM daily_briefings WHERE date_str=?", (date_str,)
        ) as cur:
            return await cur.fetchone() is not None

    async def save_daily_briefing(self, date_str: str, msg_id: int, events: list):
        await self._db.execute(
            """INSERT OR REPLACE INTO daily_briefings
               (date_str, msg_id, events_json, posted_at)
               VALUES (?, ?, ?, ?)""",
            (date_str, msg_id, json.dumps(events, ensure_ascii=False), _utcnow()),
        )
        await self._db.commit()

    async def get_daily_briefing_msg_id(self, date_str: str) -> Optional[int]:
        async with self._db.execute(
            "SELECT msg_id FROM daily_briefings WHERE date_str=?", (date_str,)
        ) as cur:
            row = await cur.fetchone()
        return row["msg_id"] if row else None

    # ── Weekly calendar ───────────────────────────────────────────────────────
    async def has_weekly_posted(self, week_key: str) -> bool:
        async with self._db.execute(
            "SELECT 1 FROM weekly_calendar WHERE week_key=?", (week_key,)
        ) as cur:
            return await cur.fetchone() is not None

    async def save_weekly_posted(self, week_key: str):
        await self._db.execute(
            "INSERT OR REPLACE INTO weekly_calendar (week_key, posted_at) VALUES (?, ?)",
            (week_key, _utcnow()),
        )
        await self._db.commit()

    # ── Reminders ────────────────────────────────────────────────────────────
    async def has_reminder_been_sent(self, event_key: str) -> bool:
        async with self._db.execute(
            "SELECT 1 FROM reminders WHERE event_key=?", (event_key,)
        ) as cur:
            return await cur.fetchone() is not None

    async def mark_reminder_sent(self, event_key: str):
        await self._db.execute(
            "INSERT OR IGNORE INTO reminders (event_key, sent_at) VALUES (?, ?)",
            (event_key, _utcnow()),
        )
        await self._db.commit()

    async def get_reminder_count_today(self, date_str: str) -> int:
        async with self._db.execute(
            "SELECT count FROM reminder_counts WHERE date_str=?", (date_str,)
        ) as cur:
            row = await cur.fetchone()
        return row["count"] if row else 0

    async def increment_reminder_count(self, date_str: str):
        await self._db.execute(
            """INSERT INTO reminder_counts (date_str, count) VALUES (?, 1)
               ON CONFLICT(date_str) DO UPDATE SET count = count + 1""",
            (date_str,),
        )
        await self._db.commit()

    # ── Motivational index ────────────────────────────────────────────────────
    async def get_and_increment_motivational_index(self) -> int:
        async with self._db.execute(
            "SELECT value FROM kv_store WHERE key='motivational_index'"
        ) as cur:
            row = await cur.fetchone()
        current = int(row["value"]) if row else 0
        next_val = current + 1
        await self._db.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES ('motivational_index', ?)",
            (str(next_val),),
        )
        await self._db.commit()
        return current

    # ── Channel offsets ───────────────────────────────────────────────────────
    async def get_last_msg_id(self, channel: str) -> int:
        async with self._db.execute(
            "SELECT last_msg_id FROM channel_offsets WHERE channel=?", (channel,)
        ) as cur:
            row = await cur.fetchone()
        return row["last_msg_id"] if row else 0

    async def set_last_msg_id(self, channel: str, msg_id: int):
        await self._db.execute(
            """INSERT INTO channel_offsets (channel, last_msg_id) VALUES (?, ?)
               ON CONFLICT(channel) DO UPDATE SET last_msg_id = ?""",
            (channel, msg_id, msg_id),
        )
        await self._db.commit()

    # ── Stats ─────────────────────────────────────────────────────────────────
    async def stats(self) -> dict:
        cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        async with self._db.execute("SELECT COUNT(*) as n FROM content_hashes") as cur:
            hashes = (await cur.fetchone())["n"]
        async with self._db.execute(
            "SELECT COUNT(*) as n FROM posted_messages WHERE posted_at > ?", (cutoff_24h,)
        ) as cur:
            posted_24h = (await cur.fetchone())["n"]
        return {
            "tracked_hashes": hashes,
            "posted_last_24h": posted_24h,
        }


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()
