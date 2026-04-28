"""
scraper.py — AXIOM INTEL channel scraper and forwarder.

Rules:
  • Watches ALL source channels — posts to ALL dest channels
  • Only real geopolitical/macro news — blocks memes, TA, charts, opinions
  • ForexFactory image posted manually in source channel →
      bot analyses it, checks it is TODAY's date, posts daily briefing
      (max 1 per day — any channel posts it, first one wins)
  • Weekly FF image posted Sunday → weekly calendar post
  • 10-min reminders for USD/Gold/FOMC red events (max 2/day)
  • Double protection: hash check + perceptual image hash + AI similarity
  • No forecast, no previous, no NOTE line anywhere
  • Every post ends with [Squad 4xx](https://t.me/Squad_4xx)
  • Times: 12-hour AM/PM EAT
"""

import asyncio
import hashlib
import io
import json
import logging
import mimetypes
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

import pytz
from PIL import Image
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

from ai_engine import AIEngine, _add_signature
from memory import MemoryManager

log = logging.getLogger("scraper")

EAT = pytz.timezone("Africa/Addis_Ababa")
_IMG_MIMES = {"image/jpeg", "image/png", "image/webp", "image/gif"}

# ── ForexFactory detection keywords ──────────────────────────────────────────
_FF_CAPTION_KEYWORDS = (
    "forexfactory", "forex factory", "calendar", "economic calendar",
    "high impact", "impact news", "weekly news", "today news",
    "today's news", "weekly calendar", "this week",
)

# ── Reminder eligibility ──────────────────────────────────────────────────────
_PRIORITY_KEYWORDS = [
    "fomc", "federal open market committee", "interest rate decision",
    "rate decision", "nfp", "non-farm payroll", "non-farm payrolls",
    "cpi", "consumer price index", "pce", "core pce", "gdp",
    "fed chair", "powell speaks", "jerome powell",
    "unemployment rate", "retail sales", "gold", "xau",
]

_FOMC_KEYWORDS = (
    "fomc", "federal open market committee", "federal reserve",
    "fed rate", "rate decision", "interest rate decision",
    "fed chair", "powell speaks", "jerome powell",
    "fomc statement", "fomc minutes", "fomc meeting", "fomc press",
)

_GOLD_KEYWORDS = ("gold", "xau")

def _is_fomc_event(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in _FOMC_KEYWORDS)

def _is_reminder_eligible(event: dict) -> bool:
    if event.get("impact") != "red":
        return False
    if _is_fomc_event(event.get("name", "")):
        return True
    currency = event.get("currency", "").upper().strip()
    name_lower = event.get("name", "").lower()
    is_usd = currency == "USD"
    is_gold = any(kw in name_lower for kw in _GOLD_KEYWORDS)
    if not (is_usd or is_gold):
        return False
    forecast = event.get("forecast", "").strip()
    previous = event.get("previous", "").strip()
    return bool(
        forecast and forecast != "—" and
        previous and previous != "—"
    )

def _is_priority_event(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in _PRIORITY_KEYWORDS)

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

def _eat_now() -> datetime:
    return datetime.now(EAT)

def _eat_today_str() -> str:
    return _eat_now().strftime("%Y-%m-%d")

def _eat_today_display() -> str:
    return _eat_now().strftime("%A, %B %d, %Y")

def _looks_like_ff_image(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(kw in t for kw in _FF_CAPTION_KEYWORDS)

def _looks_like_weekly(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(kw in t for kw in ("week", "weekly", "this week", "next week"))

# ─── Perceptual image hash (for fast duplicate detection) ─────────────────────
def _compute_perceptual_hash(image_data: bytes) -> str:
    """Return a simple perceptual hash (8x8 average hash) as hex string."""
    try:
        img = Image.open(io.BytesIO(image_data))
        # Convert to grayscale and resize to 8x8
        img = img.convert('L').resize((8, 8), Image.Resampling.LANCZOS)
        pixels = list(img.getdata())
        avg = sum(pixels) / len(pixels)
        bits = ''.join('1' if p > avg else '0' for p in pixels)
        # Return SHA256 of the bitstring to keep fixed length
        return hashlib.sha256(bits.encode()).hexdigest()[:16]
    except Exception:
        # Fallback: use first 1024 bytes of image
        return hashlib.sha256(image_data[:1024]).hexdigest()[:16]

def _extract_events_from_ff_text(text: str) -> List[dict]:
    """Extract events from AI-generated FF briefing text."""
    events = []
    pattern = re.compile(
        r"(🔴|🟠)\s+(\d{1,2}:\d{2}\s+[AP]M)\s*\|\s*([A-Z]{3}):\s*(.+)"
    )
    for line in text.splitlines():
        m = pattern.search(line)
        if m:
            emoji, time_12h, currency, name = m.groups()
            impact = "red" if emoji == "🔴" else "orange"
            try:
                dt = datetime.strptime(time_12h.strip(), "%I:%M %p")
                time_24h = dt.strftime("%H:%M")
            except ValueError:
                time_24h = ""
            events.append({
                "name": name.strip(),
                "currency": currency.strip(),
                "impact": impact,
                "time_12h": time_12h.strip(),
                "time_24h": time_24h,
                "forecast": "—",
                "previous": "—",
            })
    return events


class ChannelScraper:
    def __init__(self, config: dict, ai_engine: AIEngine, memory: MemoryManager):
        self._cfg = config
        self._ai = ai_engine
        self._mem = memory

        self._dest_channels: List[str] = config.get("dest_channels", [])
        if not self._dest_channels:
            single = config.get("dest_channel", "")
            if single:
                self._dest_channels = [single]
        if not self._dest_channels:
            raise ValueError("No destination channels configured. Set DEST_CHANNELS.")

        log.info(f"📤  Posting to {len(self._dest_channels)} destination(s): {self._dest_channels}")

        self._sources = config["source_channels"]
        self._min_delay = config["min_delay_seconds"]
        self._max_delay = config["max_delay_seconds"]
        self._lookback_hours = config["lookback_hours"]

        self._todays_vip_events: List[dict] = []

        session_string = config.get("session_string", "").strip()
        if session_string:
            session = StringSession(session_string)
            log.info("Using StringSession.")
        else:
            session = config.get("session_name", "manager_session")
            log.info(f"Using file session: {session}.session")

        self._client = TelegramClient(session, config["api_id"], config["api_hash"])

    # ── Lifecycle ─────────────────────────────────────────────────────────────
    async def start(self):
        session_string = self._cfg.get("session_string", "").strip()
        if session_string:
            await self._client.connect()
            if not await self._client.is_user_authorized():
                raise RuntimeError("StringSession invalid or expired. Run generate_session.py.")
        else:
            phone = self._cfg.get("phone", "")
            await self._client.start(phone=phone if phone else None)
        me = await self._client.get_me()
        log.info(f"✅  Logged in as: {me.first_name} (@{me.username or me.id})")

    async def stop(self):
        await self._client.disconnect()

    async def _ensure_connected(self) -> bool:
        if not self._client.is_connected():
            log.warning("Telethon disconnected — reconnecting …")
            try:
                await self._client.connect()
                if not await self._client.is_user_authorized():
                    log.error("Session expired.")
                    return False
                log.info("✅  Reconnected.")
            except Exception as exc:
                log.error(f"Reconnect failed: {exc}")
                return False
        return True

    # ── Broadcast helpers ─────────────────────────────────────────────────────
    async def _broadcast_text(self, text: str):
        sent = None
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(dest, text, parse_mode="md")
                log.info(f"  → Text sent to {dest} | msg_id={sent.id}")
            except ChatWriteForbiddenError:
                log.error(f"❌  No permission to post to {dest}.")
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s on {dest}")
                await asyncio.sleep(fwe.seconds + 3)
                try:
                    sent = await self._client.send_message(dest, text, parse_mode="md")
                except Exception as exc:
                    log.error(f"Retry failed for {dest}: {exc}")
            except Exception as exc:
                log.error(f"Send text error on {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        return sent

    async def _broadcast_file_with_caption(self, file_bytes: bytes, mime: str, caption: str):
        sent = None
        for dest in self._dest_channels:
            try:
                ext = mimetypes.guess_extension(mime) or ".png"
                buf = io.BytesIO(file_bytes)
                buf.name = f"calendar{ext}"
                buf.seek(0)
                sent = await self._client.send_file(
                    dest, buf, caption=caption, parse_mode="md", force_document=False
                )
                log.info(f"  → File sent to {dest} | msg_id={sent.id}")
            except Exception as exc:
                log.error(f"Send file error on {dest}: {exc}")
                try:
                    sent = await self._client.send_message(dest, caption, parse_mode="md")
                except Exception as exc2:
                    log.error(f"Text fallback failed for {dest}: {exc2}")
            await asyncio.sleep(1)
        return sent

    async def _broadcast_media(self, text: str, image_data: Optional[bytes], image_mime: str):
        sent = None
        for dest in self._dest_channels:
            try:
                if image_data:
                    buf = io.BytesIO(image_data)
                    ext = mimetypes.guess_extension(image_mime) or ".jpg"
                    buf.name = f"media{ext}"
                    buf.seek(0)
                    sent = await self._client.send_file(dest, buf, caption=text, parse_mode="md")
                else:
                    sent = await self._client.send_message(dest, text, parse_mode="md")
                log.info(f"  → Post sent to {dest} | msg_id={sent.id}")
            except ChatWriteForbiddenError:
                log.error(f"❌  No permission to post to {dest}.")
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s on {dest}")
                await asyncio.sleep(fwe.seconds + 3)
                try:
                    sent = await self._client.send_message(dest, text, parse_mode="md")
                except Exception:
                    pass
            except Exception as exc:
                log.error(f"Send error on {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        return sent

    # ── Main poll cycle ───────────────────────────────────────────────────────
    async def poll_and_forward(self):
        stats = await self._mem.stats()
        log.info(
            f"Poll | sources={len(self._sources)} | "
            f"hashes={stats['tracked_hashes']} | "
            f"posted_24h={stats['posted_last_24h']}"
        )
        if not await self._ensure_connected():
            log.warning("Skipping poll — not connected.")
            return

        await self._check_reminders()

        for channel in self._sources:
            try:
                await self._process_channel(channel)
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s — sleeping …")
                await asyncio.sleep(fwe.seconds + 5)
            except Exception as exc:
                log.error(f"Error on {channel}: {exc}", exc_info=True)
                await asyncio.sleep(5)

    # ── Per-channel scraping ──────────────────────────────────────────────────
    async def _process_channel(self, channel: str):
        if not await self._ensure_connected():
            return
        last_id = await self._mem.get_last_msg_id(channel)
        cutoff = None
        if last_id == 0:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=self._lookback_hours)
        new_last_id = last_id
        collected = []
        try:
            async for msg in self._client.iter_messages(
                channel, limit=50,
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
        except Exception as exc:
            log.error(f"iter_messages error on {channel}: {exc}", exc_info=True)
            await self._ensure_connected()
            return

        if not collected:
            log.debug(f"No new messages from {channel}")
            await self._mem.set_last_msg_id(channel, new_last_id)
            return

        log.info(f"📨  {len(collected)} new message(s) from {channel}")
        for msg in collected:
            await self._handle_message(msg, channel)
            await asyncio.sleep(random.uniform(2, 6))
        await self._mem.set_last_msg_id(channel, new_last_id)

    # ── Per-message handler (with enhanced duplicate detection) ───────────────
    async def _handle_message(self, msg, source_channel: str):
        text = msg.text or msg.message or ""
        image_data: Optional[bytes] = None
        image_mime = "image/jpeg"
        perceptual_hash: Optional[str] = None

        if msg.media and _is_image(msg):
            try:
                buf = io.BytesIO()
                await self._client.download_media(msg.media, file=buf)
                image_data = buf.getvalue()
                image_mime = _doc_mime(msg)
                perceptual_hash = _compute_perceptual_hash(image_data)
                log.debug(f"Image: {len(image_data):,} bytes | mime={image_mime} | phash={perceptual_hash}")
            except Exception as exc:
                log.warning(f"Image download failed: {exc}")

        # ── Route: ForexFactory calendar image (no duplicate check yet, handled separately) ──
        if image_data and (_looks_like_ff_image(text) or await self._image_looks_like_ff(image_data, image_mime)):
            is_weekly = _looks_like_weekly(text)
            await self._handle_ff_image(image_data, image_mime, text, is_weekly, source_channel, msg.id)
            return

        # ── Route: Regular news with strong duplicate detection ───────────────
        content_hash = self._mem.hash_combined(text, image_data)

        # Layer 1: Exact hash duplicate
        if await self._mem.is_duplicate(content_hash):
            log.info(f"[SKIP] Hash duplicate — {content_hash[:12]}…")
            return

        # Layer 2: Perceptual image duplicate (fast)
        if perceptual_hash and await self._mem.is_image_duplicate(perceptual_hash):
            log.info(f"[SKIP] Duplicate image (perceptual hash) — {perceptual_hash}")
            await self._mem.mark_image_seen(perceptual_hash, source_channel)
            return

        # Layer 3: AI similarity against recent posts (text + image)
        if await self._is_similar_to_recent(text, image_data, perceptual_hash):
            log.info(f"[SKIP] AI similarity — same story already posted.")
            # Mark as seen to avoid future hash duplicates
            await self._mem.mark_seen(content_hash, source=source_channel)
            if perceptual_hash:
                await self._mem.mark_image_seen(perceptual_hash, source_channel)
            return

        log.info(
            f"🔍  Analysing msg {msg.id} from {source_channel} | "
            f"text={len(text)}c | image={'✅' if image_data else '❌'}"
        )
        verdict = await self._ai.analyse(text, image_data, image_mime)
        await self._mem.mark_seen(content_hash, source=source_channel)
        if perceptual_hash:
            await self._mem.mark_image_seen(perceptual_hash, source_channel)

        if not verdict.get("approved"):
            log.info(f"[REJECTED] reason='{verdict.get('reason')}' | issues={verdict.get('issues')}")
            return

        post_text = verdict.get("formatted_text", "").strip()
        if not post_text:
            return

        delay = random.uniform(self._min_delay, self._max_delay)
        log.info(f"⏳  Waiting {delay:.1f}s before posting …")
        await asyncio.sleep(delay)
        await self._simulate_typing(len(post_text))

        sent = await self._broadcast_media(post_text, image_data, image_mime)
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
        # Store for future similarity checks (with image hash)
        await self._mem.store_recent_post(
            source_text=text[:1000],
            post_text=post_text[:1000],
            image_hash=perceptual_hash,
        )
        log.info(f"✅  Posted → msg_id={sent.id} | confidence={verdict.get('confidence')}")

    # ── ForexFactory calendar image handler (unchanged logic) ─────────────────
    async def _handle_ff_image(
        self,
        image_data: bytes,
        image_mime: str,
        caption: str,
        is_weekly: bool,
        source_channel: str,
        msg_id: int,
    ):
        today_str = _eat_today_str()
        today_display = _eat_today_display()

        if is_weekly:
            now = _eat_now()
            week_start = now + timedelta(days=(7 - now.weekday()))
            week_end = week_start + timedelta(days=4)
            week_range = f"{week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}"
            week_key = now.strftime("%Y-%W")

            if await self._mem.has_weekly_posted(week_key):
                log.info(f"[SKIP] Weekly calendar already posted this week ({week_key}).")
                return

            log.info("📆  Weekly FF image detected — analysing …")
            result = await self._ai.analyse_ff_image(
                image_data, image_mime,
                today_date=today_display,
                is_weekly=True,
                week_range=week_range,
            )

            if not result.get("approved"):
                log.info(f"[SKIP] Weekly FF image rejected: {result.get('reason')}")
                return

            post_text = result.get("formatted_text", "").strip()
            if not post_text:
                return

            post_text = _add_signature(post_text)
            sent = await self._broadcast_file_with_caption(image_data, image_mime, post_text)
            if sent:
                await self._mem.save_weekly_posted(week_key)
                log.info(f"📆  Weekly calendar posted → msg_id={sent.id}")

        else:
            if await self._mem.has_daily_briefing(today_str):
                log.info(f"[SKIP] Daily briefing already posted today ({today_str}).")
                return

            log.info("📅  Daily FF image detected — analysing …")
            result = await self._ai.analyse_ff_image(
                image_data, image_mime,
                today_date=today_display,
                is_weekly=False,
            )

            if not result.get("approved"):
                log.info(f"[SKIP] Daily FF image rejected: {result.get('reason')}")
                return

            post_text = result.get("formatted_text", "").strip()
            if not post_text:
                return

            post_text = _add_signature(post_text)
            events = _extract_events_from_ff_text(post_text)
            self._todays_vip_events = self._select_vip_events(events)
            log.info(f"VIP reminder slots: {[e.get('name') for e in self._todays_vip_events]}")

            sent = await self._broadcast_file_with_caption(image_data, image_mime, post_text)
            if sent:
                await self._mem.save_daily_briefing(today_str, sent.id, events)
                log.info(f"📅  Daily briefing posted → msg_id={sent.id}")

    async def _image_looks_like_ff(self, image_data: bytes, image_mime: str) -> bool:
        try:
            prompt = (
                "Is this image a ForexFactory.com economic calendar screenshot? "
                "Respond with JSON only: {\"is_ff\": true} or {\"is_ff\": false}"
            )
            parts = [
                {"inline_data": {"mime_type": image_mime, "data": __import__('base64').b64encode(image_data).decode()}},
                prompt,
            ]
            loop = asyncio.get_event_loop()
            resp = await asyncio.wait_for(
                loop.run_in_executor(None, lambda: self._ai._gemini_vision.generate_content(parts)),
                timeout=15,
            )
            import json as _json, re as _re
            raw = resp.text
            raw = _re.sub(r"```+(?:json)?", "", raw).strip()
            data = _json.loads(raw)
            return bool(data.get("is_ff", False))
        except Exception:
            return False

    # ── AI similarity check against recent posts (enhanced) ───────────────────
    async def _is_similar_to_recent(self, new_text: str, new_image: Optional[bytes], new_phash: Optional[str] = None) -> bool:
        """Check if new content (text+image) matches any recent post via AI."""
        try:
            recent = await self._mem.get_recent_posts(limit=30)
            for old_text, old_phash in recent:
                # Fast perceptual hash match
                if new_phash and old_phash and new_phash == old_phash:
                    log.info(f"Image hash match with recent post")
                    return True
                # AI semantic comparison (text only or multimodal if both have images)
                if not old_text:
                    continue
                # We don't have old image bytes, so we compare text and rely on AI's judgment
                # (store image bytes is too heavy; perceptual hash already caught exact matches)
                same = await self._ai.is_same_story(
                    text_a=new_text[:500],
                    text_b=old_text[:500],
                    image_a=new_image,
                    image_b=None,   # no old image bytes
                )
                if same:
                    return True
        except Exception as exc:
            log.warning(f"Similarity check error: {exc}")
        return False

    # ── VIP event selection for reminders ─────────────────────────────────────
    def _select_vip_events(self, events: List[dict]) -> List[dict]:
        eligible = [e for e in events if _is_reminder_eligible(e)]
        if not eligible:
            return []
        def sort_key(e):
            return (0 if _is_priority_event(e.get("name", "")) else 1, e.get("time_24h", "99:99"))
        vip = sorted(eligible, key=sort_key)[:2]
        return vip

    # ── Reminder scheduler ────────────────────────────────────────────────────
    async def _check_reminders(self):
        today_str = _eat_today_str()
        reminder_count = await self._mem.get_reminder_count_today(today_str)
        if reminder_count >= 2:
            return
        briefing_msg_id = await self._mem.get_daily_briefing_msg_id(today_str)
        if not briefing_msg_id or briefing_msg_id == -1:
            return

        vip_events = self._todays_vip_events
        if not vip_events:
            try:
                async with self._mem._db.execute(
                    "SELECT events_json FROM daily_briefings WHERE date_str=?", (today_str,)
                ) as cur:
                    row = await cur.fetchone()
                if row and row["events_json"]:
                    all_events = json.loads(row["events_json"])
                    vip_events = self._select_vip_events(all_events)
                    self._todays_vip_events = vip_events
            except Exception as exc:
                log.warning(f"Could not recover VIP events: {exc}")
            if not vip_events:
                return

        now = _eat_now()
        now_naive = now.replace(tzinfo=None)
        slots_left = 2 - reminder_count

        for event in vip_events:
            if slots_left <= 0:
                break
            event_key = f"{today_str}_{event.get('name', '')}_{event.get('currency', '')}"
            if await self._mem.has_reminder_been_sent(event_key):
                continue
            event_time_str = event.get("time_24h", "")
            if not event_time_str:
                continue
            try:
                event_time = datetime.strptime(
                    f"{now.strftime('%Y-%m-%d')} {event_time_str}", "%Y-%m-%d %H:%M"
                )
            except ValueError:
                continue
            minutes_until = (event_time - now_naive).total_seconds() / 60
            if 8 <= minutes_until <= 12:
                await self._send_reminder(event, event_key, briefing_msg_id, today_str)
                slots_left -= 1
                await asyncio.sleep(2)

    async def _send_reminder(
        self,
        event: dict,
        event_key: str,
        reply_to_msg_id: int,
        today_str: str,
    ):
        log.info(f"⏰  Sending 10-min reminder: {event.get('name')}")
        mot_index = await self._mem.get_and_increment_motivational_index()
        alert_text = await self._ai.generate_alert(event, motivational_index=mot_index)
        if not alert_text:
            log.error(f"Failed to generate alert for {event.get('name')}")
            return
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(
                    dest, alert_text, parse_mode="md", reply_to=reply_to_msg_id
                )
                if sent:
                    log.info(f"🚨  Reminder sent to {dest} → msg_id={sent.id}")
            except Exception as exc:
                log.error(f"Reminder send failed to {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        await self._mem.mark_reminder_sent(event_key)
        await self._mem.increment_reminder_count(today_str)

    # ── Helpers ───────────────────────────────────────────────────────────────
    async def _simulate_typing(self, text_len: int):
        duration = min(max(text_len / 180, 2), 14)
        if self._dest_channels:
            try:
                async with self._client.action(self._dest_channels[0], "typing"):
                    await asyncio.sleep(duration)
            except Exception:
                pass

    async def reminder_dispatcher_loop(self):
        log.info("🔔  Reminder dispatcher running …")
        while True:
            try:
                await self._check_reminders()
            except Exception as exc:
                log.error(f"Reminder dispatcher error: {exc}", exc_info=True)
            await asyncio.sleep(60)
