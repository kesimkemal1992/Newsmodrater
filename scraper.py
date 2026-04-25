"""
scraper.py — Telethon channel scraper, forwarder & reminder dispatcher.

Two channel types:
  • SOURCE_CHANNELS  — standard Macro/Geo news → AI analysis → formatted post
  • CALENDAR_SOURCE  — ForexFactory/calendar screenshots → vision scan
                        → if High Impact found → post + schedule 10-min reminder

Reminder loop runs as a background asyncio task.
Reminders are posted as Replies to the original calendar post.
"""

import asyncio
import io
import logging
import mimetypes
import random
import re
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

# How many minutes before the event to fire the reminder
_REMINDER_LEAD_MINUTES = 10

# Reminder template (English only, institutional style)
_REMINDER_TEMPLATE = (
    "🚨 *HIGH IMPACT NEWS REMINDER*\n\n"
    "🔴 *EVENT:* {event_name}\n"
    "⏳ *TIME LEFT:* 10 MINUTES\n\n"
    "⚠️ *CAUTION:* Extreme volatility expected. "
    "Manage your risk, move SL to break\\-even, "
    "or stay out of the market during this release\\."
)


# ══════════════════════════════════════════════════════════════════════════════
#  Media helpers
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
#  Time helpers
# ══════════════════════════════════════════════════════════════════════════════

def _parse_utc_time(time_str: str) -> Optional[datetime]:
    """
    Parse "HH:MM UTC" or "HH:MM" into a timezone-aware datetime for today (UTC).
    Returns None if parsing fails.
    """
    if not time_str:
        return None
    cleaned = time_str.upper().replace("UTC", "").strip()
    try:
        t = datetime.strptime(cleaned, "%H:%M")
        now_utc = datetime.now(timezone.utc)
        candidate = now_utc.replace(
            hour=t.hour, minute=t.minute, second=0, microsecond=0
        )
        # If the time already passed today, assume it's tomorrow
        if candidate < now_utc:
            candidate += timedelta(days=1)
        return candidate
    except ValueError:
        log.warning(f"Cannot parse time string: '{time_str}'")
        return None


def _seconds_until(dt: datetime) -> float:
    now_utc = datetime.now(timezone.utc)
    return (dt - now_utc).total_seconds()


def _escape_md(text: str) -> str:
    """Escape special chars for Telegram MarkdownV2."""
    special = r"\_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", text)


# ══════════════════════════════════════════════════════════════════════════════
#  ChannelScraper
# ══════════════════════════════════════════════════════════════════════════════

class ChannelScraper:
    def __init__(self, config: dict, ai_engine: AIEngine, memory: MemoryManager):
        self._cfg = config
        self._ai = ai_engine
        self._mem = memory
        self._dest = config["dest_channel"]
        self._sources = config["source_channels"]
        self._calendar_source = config.get("calendar_source", "")
        self._min_delay = config["min_delay_seconds"]
        self._max_delay = config["max_delay_seconds"]
        self._lookback_hours = config["lookback_hours"]

        self._reminder_task: Optional[asyncio.Task] = None

        # ── Session setup ──────────────────────────────────────────────────────
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
        session_string = self._cfg.get("session_string", "").strip()
        if session_string:
            await self._client.connect()
            if not await self._client.is_user_authorized():
                raise RuntimeError(
                    "StringSession is invalid or expired. "
                    "Run generate_session.py locally to get a new one."
                )
        else:
            phone = self._cfg.get("phone", "")
            await self._client.start(phone=phone if phone else None)

        me = await self._client.get_me()
        log.info(f"✅  Logged in as: {me.first_name} (@{me.username or me.id})")

        # Start background reminder dispatcher
        self._reminder_task = asyncio.create_task(
            self._reminder_dispatcher_loop(),
            name="reminder_dispatcher",
        )
        log.info("⏰  Reminder dispatcher started.")

    async def stop(self):
        if self._reminder_task and not self._reminder_task.done():
            self._reminder_task.cancel()
            try:
                await self._reminder_task
            except asyncio.CancelledError:
                pass
        await self._client.disconnect()

    # ── Main poll cycle ────────────────────────────────────────────────────────

    async def poll_and_forward(self):
        stats = await self._mem.stats()
        log.info(
            f"Poll cycle | sources={len(self._sources)} | "
            f"calendar={'✅' if self._calendar_source else '❌'} | "
            f"hashes={stats['tracked_hashes']} | "
            f"posted_24h={stats['posted_last_24h']} | "
            f"pending_reminders={stats['pending_reminders']}"
        )

        # Process calendar source first (higher priority)
        if self._calendar_source:
            try:
                await self._process_channel(
                    self._calendar_source, is_calendar=True
                )
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s on calendar — sleeping …")
                await asyncio.sleep(fwe.seconds + 5)
            except Exception as exc:
                log.error(f"Error on calendar channel: {exc}", exc_info=True)

        # Process regular news channels
        for channel in self._sources:
            try:
                await self._process_channel(channel, is_calendar=False)
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s — sleeping …")
                await asyncio.sleep(fwe.seconds + 5)
            except Exception as exc:
                log.error(f"Error on channel {channel}: {exc}", exc_info=True)
                await asyncio.sleep(5)

    async def _process_channel(self, channel: str, is_calendar: bool = False):
        last_id = await self._mem.get_last_msg_id(channel)
        cutoff = None
        if last_id == 0:
            cutoff = datetime.now(timezone.utc) - timedelta(
                hours=self._lookback_hours
            )

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

        tag = "📅 CALENDAR" if is_calendar else "📨 NEWS"
        log.info(f"{tag}  {len(collected)} new message(s) from {channel}")

        for msg in collected:
            if is_calendar:
                await self._handle_calendar_message(msg, channel)
            else:
                await self._handle_news_message(msg, channel)
            await asyncio.sleep(random.uniform(2, 6))

        await self._mem.set_last_msg_id(channel, new_last_id)

    # ── Calendar message handler ───────────────────────────────────────────────

    async def _handle_calendar_message(self, msg, source_channel: str):
        """
        Processes a message from CALENDAR_SOURCE.
        - Must have an image (calendar screenshot).
        - Runs Gemini calendar vision scan.
        - If High Impact found: post + schedule reminder.
        """
        text = msg.text or msg.message or ""
        image_data: Optional[bytes] = None
        image_mime = "image/jpeg"

        # Calendar messages MUST have an image
        if not (msg.media and _is_image(msg)):
            log.debug(f"[CALENDAR] No image in msg {msg.id} — skip.")
            return

        try:
            buf = io.BytesIO()
            await self._client.download_media(msg.media, file=buf)
            image_data = buf.getvalue()
            image_mime = _doc_mime(msg)
            log.debug(
                f"[CALENDAR] Image downloaded: {len(image_data):,} bytes | mime={image_mime}"
            )
        except Exception as exc:
            log.warning(f"[CALENDAR] Image download failed: {exc}")
            return

        # Dedup check on image hash
        content_hash = self._mem.hash_combined(text, image_data)
        if await self._mem.is_duplicate(content_hash):
            log.info(f"[CALENDAR] Duplicate screenshot — hash={content_hash[:12]}… skip.")
            return

        # Mark seen immediately to prevent re-processing
        await self._mem.mark_seen(content_hash, source=source_channel)

        # Run calendar vision scan
        log.info(f"📅  Scanning calendar image from msg {msg.id} …")
        cal_result = await self._ai.analyse_calendar(image_data, image_mime, text)

        if not cal_result.get("is_high_impact"):
            log.info(
                f"[CALENDAR] No High Impact event found | "
                f"reason='{cal_result.get('reason')}'"
            )
            return

        event_name = cal_result.get("event_name", "Unknown Event")
        news_time = cal_result.get("news_time", "")

        log.info(
            f"🔴  HIGH IMPACT detected: '{event_name}' @ {news_time} | "
            f"engine={cal_result.get('engine')}"
        )

        # Validate and check timing
        news_dt = _parse_utc_time(news_time)
        if news_dt is None:
            log.warning(f"[CALENDAR] Cannot parse time '{news_time}' — posting without reminder.")
            await self._post_calendar_image(
                image_data, image_mime, event_name, news_time, msg.id, text
            )
            return

        seconds_left = _seconds_until(news_dt)
        reminder_fire_seconds = seconds_left - (_REMINDER_LEAD_MINUTES * 60)

        # Build and post the calendar announcement
        dest_msg = await self._post_calendar_image(
            image_data, image_mime, event_name, news_time, msg.id, text
        )

        if dest_msg is None:
            log.error("[CALENDAR] Failed to post — reminder not scheduled.")
            return

        # Safety checks on timing
        if seconds_left <= 0:
            log.info(
                f"[CALENDAR] Event '{event_name}' already passed "
                f"({abs(seconds_left):.0f}s ago) — no reminder."
            )
            return

        if reminder_fire_seconds <= 0:
            log.info(
                f"[CALENDAR] '{event_name}' in {seconds_left/60:.1f} min — "
                f"less than {_REMINDER_LEAD_MINUTES} min window, no reminder."
            )
            return

        # Schedule reminder in DB
        scheduled = await self._mem.schedule_reminder(
            event_name=event_name,
            news_time_utc=news_time,
            dest_msg_id=dest_msg.id,
        )

        if scheduled:
            log.info(
                f"⏰  Reminder scheduled for '{event_name}' — "
                f"fires in {reminder_fire_seconds/60:.1f} min "
                f"(reply to msg {dest_msg.id})"
            )
        else:
            log.info(f"[CALENDAR] Duplicate reminder for '{event_name}' — skipped.")

    async def _post_calendar_image(
        self,
        image_data: bytes,
        image_mime: str,
        event_name: str,
        news_time: str,
        source_msg_id: int,
        original_text: str,
    ):
        """
        Post the calendar screenshot with a formatted caption.
        Returns the sent Message object, or None on failure.
        """
        # Format caption
        caption = (
            f"📅 *ECONOMIC CALENDAR — HIGH IMPACT*\n\n"
            f"🔴 *EVENT:* {_escape_md(event_name)}\n"
            f"🕐 *TIME:* {_escape_md(news_time)}\n\n"
            f"⚠️ Reminder will fire *10 minutes before* this release\\."
        )

        delay = random.uniform(self._min_delay, self._max_delay)
        log.info(f"[CALENDAR] Waiting {delay:.1f}s before posting …")
        await asyncio.sleep(delay)

        try:
            buf = io.BytesIO(image_data)
            ext = mimetypes.guess_extension(image_mime) or ".jpg"
            buf.name = f"calendar{ext}"
            sent = await self._client.send_file(
                self._dest, buf, caption=caption, parse_mode="md"
            )
            log.info(f"[CALENDAR] Posted → msg_id={sent.id}")
            return sent
        except ChatWriteForbiddenError:
            log.error("❌  Cannot post to destination — check admin rights.")
        except FloodWaitError as fwe:
            log.warning(f"FloodWait {fwe.seconds}s while posting calendar — retrying …")
            await asyncio.sleep(fwe.seconds + 3)
            return await self._post_calendar_image(
                image_data, image_mime, event_name, news_time,
                source_msg_id, original_text
            )
        except Exception as exc:
            log.error(f"[CALENDAR] Send error: {exc}", exc_info=True)
        return None

    # ── News message handler ───────────────────────────────────────────────────

    async def _handle_news_message(self, msg, source_channel: str):
        text = msg.text or msg.message or ""
        image_data: Optional[bytes] = None
        image_mime = "image/jpeg"

        if msg.media and _is_image(msg):
            try:
                buf = io.BytesIO()
                await self._client.download_media(msg.media, file=buf)
                image_data = buf.getvalue()
                image_mime = _doc_mime(msg)
                log.debug(
                    f"Image downloaded: {len(image_data):,} bytes | mime={image_mime}"
                )
            except Exception as exc:
                log.warning(f"Image download failed: {exc}")

        # Dedup
        content_hash = self._mem.hash_combined(text, image_data)
        if await self._mem.is_duplicate(content_hash):
            log.info(f"[SKIP] Duplicate — hash={content_hash[:12]}…")
            return

        log.info(
            f"🔍  Analysing msg {msg.id} from {source_channel} | "
            f"text={len(text)}c | image={'✅' if image_data else '❌'}"
        )
        verdict = await self._ai.analyse(text, image_data, image_mime)

        # Mark seen regardless of verdict
        await self._mem.mark_seen(content_hash, source=source_channel)

        if not verdict.get("approved"):
            log.info(
                f"[REJECTED] engine={verdict.get('engine')} | "
                f"reason='{verdict.get('reason')}' | "
                f"issues={verdict.get('issues')}"
            )
            return

        post_text = self._build_news_post(verdict)

        delay = random.uniform(self._min_delay, self._max_delay)
        log.info(f"⏳  Waiting {delay:.1f}s before posting …")
        await asyncio.sleep(delay)

        await self._simulate_typing(len(post_text))

        sent = await self._send_message(post_text, image_data, image_mime)
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

    # ── Reminder dispatcher (background loop) ──────────────────────────────────

    async def _reminder_dispatcher_loop(self):
        """
        Background task. Checks pending reminders every 60 seconds.
        When a reminder is due (10 min before event), fires it as a Reply.
        """
        log.info("⏰  Reminder dispatcher loop running …")
        while True:
            try:
                await self._check_and_fire_reminders()
            except asyncio.CancelledError:
                log.info("⏰  Reminder dispatcher cancelled.")
                break
            except Exception as exc:
                log.error(f"Reminder dispatcher error: {exc}", exc_info=True)
            await asyncio.sleep(60)  # check every minute

    async def _check_and_fire_reminders(self):
        pending = await self._mem.get_pending_reminders()
        if not pending:
            return

        now_utc = datetime.now(timezone.utc)
        log.debug(f"[REMINDER] Checking {len(pending)} pending reminder(s) …")

        for reminder in pending:
            rid = reminder["id"]
            event_name = reminder["event_name"]
            news_time = reminder["news_time_utc"]
            dest_msg_id = reminder["dest_msg_id"]

            news_dt = _parse_utc_time(news_time)
            if news_dt is None:
                log.warning(
                    f"[REMINDER] Cannot parse time for '{event_name}' — marking skipped."
                )
                await self._mem.mark_reminder_sent(rid, status=2)
                continue

            seconds_left = _seconds_until(news_dt)

            # Already past — skip
            if seconds_left <= 0:
                log.info(
                    f"[REMINDER] '{event_name}' already passed — marking skipped."
                )
                await self._mem.mark_reminder_sent(rid, status=2)
                continue

            # Too early — not yet in the 10-min window
            reminder_fire_at = seconds_left - (_REMINDER_LEAD_MINUTES * 60)
            if reminder_fire_at > 0:
                log.debug(
                    f"[REMINDER] '{event_name}' fires in "
                    f"{reminder_fire_at/60:.1f} min — waiting."
                )
                continue

            # Window hit — fire the reminder
            log.info(
                f"🔔  FIRING reminder for '{event_name}' "
                f"({seconds_left/60:.1f} min to event) → reply to msg {dest_msg_id}"
            )
            fired = await self._send_reminder(event_name, dest_msg_id)
            if fired:
                await self._mem.mark_reminder_sent(rid, status=1)
                log.info(f"[REMINDER] ✅ Sent for '{event_name}'")
            else:
                log.error(f"[REMINDER] ❌ Failed to send for '{event_name}'")

    async def _send_reminder(self, event_name: str, reply_to_msg_id: int):
        """Send the reminder as a Reply to the original calendar post."""
        text = _REMINDER_TEMPLATE.format(event_name=_escape_md(event_name))
        try:
            sent = await self._client.send_message(
                self._dest,
                text,
                parse_mode="md",
                reply_to=reply_to_msg_id,
            )
            return sent
        except ChatWriteForbiddenError:
            log.error("❌  Cannot post reminder — check admin rights.")
        except FloodWaitError as fwe:
            log.warning(f"FloodWait {fwe.seconds}s during reminder — retrying …")
            await asyncio.sleep(fwe.seconds + 3)
            return await self._send_reminder(event_name, reply_to_msg_id)
        except Exception as exc:
            log.error(f"Reminder send error: {exc}", exc_info=True)
        return None

    # ── Shared helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _build_news_post(verdict: dict) -> str:
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

    async def _send_message(
        self,
        text: str,
        image_data: Optional[bytes],
        image_mime: str,
        reply_to: Optional[int] = None,
    ):
        """Send a message, with iterative FloodWait retry (not recursive)."""
        for attempt in range(3):
            try:
                if image_data:
                    buf = io.BytesIO(image_data)
                    ext = mimetypes.guess_extension(image_mime) or ".jpg"
                    buf.name = f"media{ext}"
                    return await self._client.send_file(
                        self._dest,
                        buf,
                        caption=text,
                        parse_mode="md",
                        reply_to=reply_to,
                    )
                else:
                    return await self._client.send_message(
                        self._dest,
                        text,
                        parse_mode="md",
                        reply_to=reply_to,
                    )
            except ChatWriteForbiddenError:
                log.error("❌  Cannot post to destination — check admin rights.")
                return None
            except FloodWaitError as fwe:
                log.warning(
                    f"FloodWait {fwe.seconds}s while sending "
                    f"(attempt {attempt+1}/3) — waiting …"
                )
                await asyncio.sleep(fwe.seconds + 3)
            except Exception as exc:
                log.error(f"Send error: {exc}", exc_info=True)
                return None
        log.error("Send failed after 3 FloodWait retries.")
        return None
