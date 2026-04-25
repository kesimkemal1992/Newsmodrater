"""
scraper.py — Telethon channel scraper & forwarder.

Supports both StringSession (Railway/Render) and file session (local dev).
StringSession is preferred for server deployment — no OTP prompt needed.
"""

import asyncio
import io
import logging
import mimetypes
import random
from datetime import datetime, timedelta, timezone
from typing import Optional

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

from ai_engine import AIEngine
from memory import MemoryManager

log = logging.getLogger("scraper")

_IMG_MIMES = {"image/jpeg", "image/png", "image/webp", "image/gif"}


def _is_image(msg) -> bool:
    if isinstance(msg.media, MessageMediaPhoto):
        return True
    if isinstance(msg.media, MessageMediaDocument):
        doc = msg.media.document
        if doc and doc.mime_type in _IMG_MIMES:
            return True
    return False


def _doc_mime(msg) -> str:
    if isinstance(msg.media, MessageMediaDocument):
        return msg.media.document.mime_type or "image/jpeg"
    return "image/jpeg"


class ChannelScraper:
    def __init__(self, config: dict, ai_engine: AIEngine, memory: MemoryManager):
        self._cfg = config
        self._ai = ai_engine
        self._mem = memory
        self._dest = config["dest_channel"]
        self._sources = config["source_channels"]
        self._min_delay = config["min_delay_seconds"]
        self._max_delay = config["max_delay_seconds"]
        self._lookback_hours = config["lookback_hours"]

        # ── Session: StringSession preferred, file session as fallback ─────────
        session_string = config.get("session_string", "").strip()
        if session_string:
            session = StringSession(session_string)
            log.info("Using StringSession for authentication.")
        else:
            session = config.get("session_name", "manager_session")
            log.info(f"Using file session: {session}.session")

        self._client = TelegramClient(
            session,
            config["api_id"],
            config["api_hash"],
        )

    # ── Lifecycle ──────────────────────────────────────────────────────────────
    async def start(self):
        """Connect using StringSession — no phone/OTP prompt on server."""
        session_string = self._cfg.get("session_string", "").strip()

        if session_string:
            # StringSession: connect directly, no interaction needed
            await self._client.connect()
            if not await self._client.is_user_authorized():
                raise RuntimeError(
                    "StringSession is invalid or expired. "
                    "Run generate_session.py locally to get a new one."
                )
        else:
            # File session fallback (local dev only)
            phone = self._cfg.get("phone", "")
            await self._client.start(phone=phone if phone else None)

        me = await self._client.get_me()
        log.info(f"✅  Logged in as: {me.first_name} (@{me.username or me.id})")

    async def stop(self):
        await self._client.disconnect()

    # ── Main poll cycle ────────────────────────────────────────────────────────
    async def poll_and_forward(self):
        stats = await self._mem.stats()
        log.info(
            f"Poll cycle | sources={len(self._sources)} | "
            f"hashes={stats['tracked_hashes']} | "
            f"posted_24h={stats['posted_last_24h']}"
        )

        for channel in self._sources:
            try:
                await self._process_channel(channel)
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s — sleeping …")
                await asyncio.sleep(fwe.seconds + 5)
            except Exception as exc:
                log.error(f"Error on channel {channel}: {exc}", exc_info=True)
                await asyncio.sleep(5)

    async def _process_channel(self, channel: str):
        last_id = await self._mem.get_last_msg_id(channel)

        cutoff = None
        if last_id == 0:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=self._lookback_hours)

        new_last_id = last_id
        collected = []

        async for msg in self._client.iter_messages(
            channel,
            limit=50,
            min_id=last_id if last_id else 0,
            offset_date=cutoff,
            reverse=True,
        ):
            if msg.id <= last_id:
                continue
            if not (msg.text or msg.media):
                continue
            collected.append(msg)
            new_last_id = max(new_last_id, msg.id)

        if not collected:
            log.debug(f"No new messages from {channel}")
            await self._mem.set_last_msg_id(channel, new_last_id)
            return

        log.info(f"📨  {len(collected)} new message(s) from {channel}")

        for msg in collected:
            await self._handle_message(msg, channel)
            await asyncio.sleep(random.uniform(2, 6))

        await self._mem.set_last_msg_id(channel, new_last_id)

    # ── Per-message handler ────────────────────────────────────────────────────
    async def _handle_message(self, msg, source_channel: str):
        text = msg.text or msg.message or ""
        image_data: Optional[bytes] = None
        image_mime = "image/jpeg"

        # Download image into memory (never to disk)
        if msg.media and _is_image(msg):
            try:
                buf = io.BytesIO()
                await self._client.download_media(msg.media, file=buf)
                image_data = buf.getvalue()
                image_mime = _doc_mime(msg)
                log.debug(f"Image downloaded: {len(image_data):,} bytes | mime={image_mime}")
            except Exception as exc:
                log.warning(f"Image download failed: {exc}")

        # ── Deduplication ──────────────────────────────────────────────────────
        content_hash = self._mem.hash_combined(text, image_data)
        if await self._mem.is_duplicate(content_hash):
            log.info(f"[SKIP] Duplicate — hash={content_hash[:12]}…")
            return

        # ── AI Analysis ────────────────────────────────────────────────────────
        log.info(
            f"🔍  Analysing msg {msg.id} from {source_channel} | "
            f"text={len(text)}c | image={'✅' if image_data else '❌'}"
        )
        verdict = await self._ai.analyse(text, image_data, image_mime)

        # Mark seen regardless of verdict (prevents re-analysis on next poll)
        await self._mem.mark_seen(content_hash, source=source_channel)

        if not verdict.get("approved"):
            log.info(
                f"[REJECTED] engine={verdict.get('engine')} | "
                f"reason='{verdict.get('reason')}' | "
                f"issues={verdict.get('issues')}"
            )
            return

        # ── Assemble final post ────────────────────────────────────────────────
        post_text = self._build_post(verdict)

        # ── Human-like delay ───────────────────────────────────────────────────
        delay = random.uniform(self._min_delay, self._max_delay)
        log.info(f"⏳  Waiting {delay:.1f}s before posting …")
        await asyncio.sleep(delay)

        # ── Typing simulation ──────────────────────────────────────────────────
        await self._simulate_typing(len(post_text))

        # ── Send ───────────────────────────────────────────────────────────────
        sent = await self._send(post_text, image_data, image_mime)
        if sent is None:
            return

        await self._mem.log_posted(
            source_channel=source_channel,
            source_msg_id=msg.id,
            dest_msg_id=sent.id,
            content_hash=content_hash,
            ai_verdict=verdict,
            formatted_text=post_text,
        )
        log.info(
            f"✅  Posted → msg_id={sent.id} | "
            f"engine={verdict.get('engine')} | "
            f"confidence={verdict.get('confidence')}"
        )

    # ── Helpers ────────────────────────────────────────────────────────────────
    @staticmethod
    def _build_post(verdict: dict) -> str:
        body = verdict.get("formatted_text", "").strip()
        tags = verdict.get("hashtags", "").strip()
        if tags and not body.endswith(tags):
            body = f"{body}\n\n{tags}"
        return body

    async def _simulate_typing(self, text_len: int):
        duration = min(max(text_len / 180, 2), 14)
        try:
            async with self._client.action(self._dest, "typing"):
                await asyncio.sleep(duration)
        except Exception as exc:
            log.debug(f"Typing action skipped: {exc}")

    async def _send(
        self,
        text: str,
        image_data: Optional[bytes],
        image_mime: str,
    ):
        try:
            if image_data:
                buf = io.BytesIO(image_data)
                ext = mimetypes.guess_extension(image_mime) or ".jpg"
                buf.name = f"media{ext}"
                return await self._client.send_file(
                    self._dest, buf, caption=text, parse_mode="md"
                )
            else:
                return await self._client.send_message(
                    self._dest, text, parse_mode="md"
                )
        except ChatWriteForbiddenError:
            log.error("❌  Cannot post to destination channel — check admin rights.")
        except FloodWaitError as fwe:
            log.warning(f"FloodWait {fwe.seconds}s while sending — retrying …")
            await asyncio.sleep(fwe.seconds + 3)
            return await self._send(text, image_data, image_mime)
        except Exception as exc:
            log.error(f"Send error: {exc}", exc_info=True)
        return None
