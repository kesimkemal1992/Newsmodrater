"""
scraper.py — Telethon-based channel scraper & forwarder.

Monitors source channels, runs AI analysis on each new message,
and forwards approved content to the destination channel with
human-like behaviour (random delays, typing indicator).
"""

import asyncio
import io
import logging
import mimetypes
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from telethon import TelegramClient, events
from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    InputPeerChannel,
)
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

from ai_engine import AIEngine
from memory import MemoryManager

log = logging.getLogger("scraper")

# ─── MIME helpers ──────────────────────────────────────────────────────────────
_IMG_MIMES = {"image/jpeg", "image/png", "image/webp", "image/gif"}


def _is_image_media(msg) -> bool:
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

        self._client = TelegramClient(
            config["session_name"],
            config["api_id"],
            config["api_hash"],
        )
        self._dest = config["dest_channel"]
        self._sources = config["source_channels"]
        self._min_delay = config["min_delay_seconds"]
        self._max_delay = config["max_delay_seconds"]
        self._lookback_hours = config["lookback_hours"]

    # ── Lifecycle ──────────────────────────────────────────────────────────────
    async def start(self):
        await self._client.start(phone=self._cfg["phone"])
        me = await self._client.get_me()
        log.info(f"Logged in as: {me.first_name} ({me.username or me.id})")

    async def stop(self):
        await self._client.disconnect()

    # ── Main poll cycle ────────────────────────────────────────────────────────
    async def poll_and_forward(self):
        log.info(f"Poll cycle started — watching {len(self._sources)} channels")
        stats = await self._mem.stats()
        log.info(
            f"Memory: {stats['tracked_hashes']} hashes | "
            f"{stats['posted_last_24h']} posts in last 24h"
        )

        for channel in self._sources:
            try:
                await self._process_channel(channel)
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s — sleeping …")
                await asyncio.sleep(fwe.seconds + 5)
            except Exception as exc:
                log.error(f"Error processing {channel}: {exc}", exc_info=True)
                await asyncio.sleep(5)

    async def _process_channel(self, channel: str):
        last_id = await self._mem.get_last_msg_id(channel)

        # First run: look back N hours instead of from 0
        if last_id == 0:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=self._lookback_hours)
        else:
            cutoff = None  # use message ID as boundary

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

        log.info(f"Found {len(collected)} new message(s) from {channel}")

        for msg in collected:
            await self._handle_message(msg, channel)
            # Human-like inter-message delay
            await asyncio.sleep(random.uniform(2, 8))

        await self._mem.set_last_msg_id(channel, new_last_id)

    # ── Message handler ────────────────────────────────────────────────────────
    async def _handle_message(self, msg, source_channel: str):
        text = msg.text or msg.message or ""
        image_data: Optional[bytes] = None
        image_mime = "image/jpeg"

        # Download image if present
        if msg.media and _is_image_media(msg):
            try:
                buf = io.BytesIO()
                await self._client.download_media(msg.media, file=buf)
                image_data = buf.getvalue()
                image_mime = _doc_mime(msg)
                log.debug(f"Downloaded image: {len(image_data)} bytes, mime={image_mime}")
            except Exception as exc:
                log.warning(f"Failed to download image: {exc}")

        # --- Deduplication ---
        content_hash = self._mem.hash_combined(text, image_data)
        if await self._mem.is_duplicate(content_hash):
            log.info(f"[SKIP] Duplicate content (hash={content_hash[:12]}…)")
            return

        # --- AI Analysis ---
        log.info(
            f"Analysing msg {msg.id} from {source_channel} "
            f"[text={len(text)}chars, image={'yes' if image_data else 'no'}]"
        )
        verdict = await self._ai.analyse(text, image_data, image_mime)

        # Mark seen regardless of approval (prevent re-analysis)
        await self._mem.mark_seen(content_hash, source=source_channel)

        if not verdict.get("approved"):
            log.info(
                f"[REJECTED] {verdict.get('reason')} | "
                f"issues={verdict.get('issues')} | engine={verdict.get('engine')}"
            )
            return

        # --- Format final post ---
        post_text = self._assemble_post(verdict)

        # --- Human delay before posting ---
        delay = random.uniform(self._min_delay, self._max_delay)
        log.info(f"Waiting {delay:.1f}s before posting (human delay) …")
        await asyncio.sleep(delay)

        # --- Simulate typing ---
        await self._simulate_typing(len(post_text))

        # --- Send to destination ---
        dest_msg = await self._send(post_text, image_data, image_mime)
        if dest_msg is None:
            return

        await self._mem.log_posted(
            source_channel=source_channel,
            source_msg_id=msg.id,
            dest_msg_id=dest_msg.id,
            content_hash=content_hash,
            ai_verdict=verdict,
            formatted_text=post_text,
        )
        log.info(
            f"✅  Posted msg {dest_msg.id} to {self._dest} | "
            f"engine={verdict.get('engine')} | confidence={verdict.get('confidence')}"
        )

    # ── Post assembly ─────────────────────────────────────────────────────────
    @staticmethod
    def _assemble_post(verdict: dict) -> str:
        body = verdict.get("formatted_text", "").strip()
        hashtags = verdict.get("hashtags", "").strip()

        if hashtags and not body.endswith(hashtags):
            # Ensure hashtags are on their own line at the bottom
            body = f"{body}\n\n{hashtags}"
        return body

    # ── Human-like typing simulation ──────────────────────────────────────────
    async def _simulate_typing(self, text_len: int):
        """Send typing action for a realistic duration based on text length."""
        # Average human types ~200 chars/min in Telegram, but we're smarter.
        typing_seconds = min(max(text_len / 200, 2), 15)
        try:
            async with self._client.action(self._dest, "typing"):
                log.debug(f"Typing … ({typing_seconds:.1f}s)")
                await asyncio.sleep(typing_seconds)
        except Exception as exc:
            log.debug(f"Typing action failed (non-critical): {exc}")

    # ── Send ───────────────────────────────────────────────────────────────────
    async def _send(
        self,
        text: str,
        image_data: Optional[bytes],
        image_mime: str,
    ):
        try:
            if image_data:
                file = io.BytesIO(image_data)
                ext = mimetypes.guess_extension(image_mime) or ".jpg"
                file.name = f"media{ext}"
                msg = await self._client.send_file(
                    self._dest,
                    file,
                    caption=text,
                    parse_mode="md",
                )
            else:
                msg = await self._client.send_message(
                    self._dest,
                    text,
                    parse_mode="md",
                )
            return msg

        except ChatWriteForbiddenError:
            log.error("Cannot write to destination channel — check permissions!")
        except FloodWaitError as fwe:
            log.warning(f"FloodWait {fwe.seconds}s while sending — sleeping …")
            await asyncio.sleep(fwe.seconds + 3)
            # Retry once
            return await self._send(text, image_data, image_mime)
        except Exception as exc:
            log.error(f"Send failed: {exc}", exc_info=True)
        return None
