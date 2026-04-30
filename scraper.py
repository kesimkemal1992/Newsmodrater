"""
scraper.py — AXIOM INTEL channel scraper and forwarder.
- Only USD high‑impact (🔴) events trigger reminders (no 🟠)
- Geopolitical events are filtered out from daily calendar briefing.
- Reminders reply to daily briefing post.
- Professional motivational lines from ai_engine.
"""

import asyncio
import io
import json
import logging
import mimetypes
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple

import pytz
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

from ai_engine import AIEngine, _add_signature
from memory import MemoryManager

log = logging.getLogger("scraper")

EAT = pytz.timezone("Africa/Addis_Ababa")
_IMG_MIMES = {"image/jpeg", "image/png", "image/webp", "image/gif"}

_FF_CAPTION_KEYWORDS = (
    "forexfactory", "forex factory", "calendar", "economic calendar",
    "high impact", "impact news", "weekly news", "today news",
    "today's news", "weekly calendar", "this week",
    "fomc", "federal funds rate", "interest rate decision"
)

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

_UNIQUE_EVENT_NAMES = [
    "federal funds rate", "interest rate decision",
    "non-farm payroll", "nfp", "consumer price index", "cpi",
    "pce", "gdp", "fed chair", "powell speaks"
]

# Only truly market-moving red events (high liquidity, high volatility)
_HIGH_IMPACT_KEYWORDS = [
    "fomc", "federal funds rate", "interest rate decision",
    "non-farm payroll", "nfp", "consumer price index", "cpi",
    "pce", "core pce", "gdp", "advance gdp", "preliminary gdp",
    "fed chair", "powell speaks", "unemployment rate", "retail sales",
    "ism manufacturing", "ism non-manufacturing"
]

# Geopolitical keywords to filter out from calendar briefing
GEOPOLITICAL_KEYWORDS = [
    "trump", "iran", "hormuz", "war", "missile", "strike", "attack",
    "geopolitical", "oil supply", "ukraine", "russia", "biden", "putin", "xi"
]

def _is_fomc_event(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in _FOMC_KEYWORDS)

def _is_reminder_eligible(event: dict) -> bool:
    # Only red impact
    if event.get("impact") != "red":
        return False
    name_lower = event.get("name", "").lower()
    # Exclude geopolitical events (no reminder for these)
    if any(kw in name_lower for kw in GEOPOLITICAL_KEYWORDS):
        return False
    # Only high-impact economic events
    for kw in _HIGH_IMPACT_KEYWORDS:
        if kw in name_lower:
            return True
    return False

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

def _normalise_urls(text: str) -> str:
    if not text:
        return text
    pattern = r'(\?|&)(utm_[^&]+|fbclid=[^&]+|ref=[^&]+|source=[^&]+)'
    return re.sub(pattern, '', text)

def _extract_events_from_ff_text(text: str) -> List[dict]:
    """
    Extract events from AI‑generated calendar text, group same time, convert to 24h.
    """
    events = []
    pattern = re.compile(r"(🔴|🟠)\s+(\d{1,2}:\d{2}\s+[AP]M)\s*\|\s*([A-Z]{3}):\s*(.+)")
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
    # Group same time (by time_24h) – combine names with commas
    grouped = {}
    for e in events:
        key = (e["time_24h"], e["impact"], e["currency"])
        if key not in grouped:
            grouped[key] = e.copy()
            grouped[key]["name"] = [e["name"]]
        else:
            grouped[key]["name"].append(e["name"])
    result = []
    for key, e in grouped.items():
        e["name"] = ", ".join(e["name"]) if isinstance(e["name"], list) else e["name"]
        result.append(e)
    return result

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
            raise ValueError("No destination channels configured.")
        log.info(f"📤 Posting to {len(self._dest_channels)} destination(s): {self._dest_channels}")

        self._sources = config["source_channels"]
        self._min_delay = config["min_delay_seconds"]
        self._max_delay = config["max_delay_seconds"]
        self._lookback_hours = config["lookback_hours"]

        self._todays_vip_events: List[dict] = []

        session_string = config.get("session_string", "").strip()
        if session_string:
            session = StringSession(session_string)
        else:
            session = config.get("session_name", "manager_session")
        self._client = TelegramClient(session, config["api_id"], config["api_hash"])

    async def start(self):
        session_string = self._cfg.get("session_string", "").strip()
        if session_string:
            await self._client.connect()
            if not await self._client.is_user_authorized():
                raise RuntimeError("StringSession invalid or expired.")
        else:
            phone = self._cfg.get("phone", "")
            await self._client.start(phone=phone if phone else None)
        me = await self._client.get_me()
        log.info(f"✅ Logged in as: {me.first_name} (@{me.username or me.id})")

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
                log.info("✅ Reconnected.")
            except Exception as exc:
                log.error(f"Reconnect failed: {exc}")
                return False
        return True

    async def _broadcast_text(self, text: str):
        sent = None
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(dest, text, parse_mode="md")
                log.info(f"  → Text sent to {dest} | msg_id={sent.id}")
            except ChatWriteForbiddenError:
                log.error(f"❌ No permission to post to {dest}.")
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

    async def _broadcast_file_with_caption(self, file_bytes: bytes, mime: str, caption: str, reply_to: int = None):
        sent = None
        for dest in self._dest_channels:
            try:
                ext = mimetypes.guess_extension(mime) or ".png"
                buf = io.BytesIO(file_bytes)
                buf.name = f"calendar{ext}"
                buf.seek(0)
                sent = await self._client.send_file(dest, buf, caption=caption, parse_mode="md", force_document=False, reply_to=reply_to)
                log.info(f"  → File sent to {dest} | msg_id={sent.id}")
            except Exception as exc:
                log.error(f"Send file error on {dest}: {exc}")
                try:
                    sent = await self._client.send_message(dest, caption, parse_mode="md", reply_to=reply_to)
                except Exception as exc2:
                    log.error(f"Text fallback failed for {dest}: {exc2}")
            await asyncio.sleep(1)
        return sent

    async def _broadcast_media(self, text: str, image_data: Optional[bytes], image_mime: str, reply_to: int = None):
        sent = None
        for dest in self._dest_channels:
            try:
                if image_data:
                    buf = io.BytesIO(image_data)
                    ext = mimetypes.guess_extension(image_mime) or ".jpg"
                    buf.name = f"media{ext}"
                    buf.seek(0)
                    sent = await self._client.send_file(dest, buf, caption=text, parse_mode="md", reply_to=reply_to)
                else:
                    sent = await self._client.send_message(dest, text, parse_mode="md", reply_to=reply_to)
                log.info(f"  → Post sent to {dest} | msg_id={sent.id}")
            except ChatWriteForbiddenError:
                log.error(f"❌ No permission to post to {dest}.")
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s on {dest}")
                await asyncio.sleep(fwe.seconds + 3)
                try:
                    if image_data:
                        buf = io.BytesIO(image_data)
                        ext = mimetypes.guess_extension(image_mime) or ".jpg"
                        buf.name = f"media{ext}"
                        buf.seek(0)
                        sent = await self._client.send_file(dest, buf, caption=text, parse_mode="md", reply_to=reply_to)
                    else:
                        sent = await self._client.send_message(dest, text, parse_mode="md", reply_to=reply_to)
                except Exception:
                    pass
            except Exception as exc:
                log.error(f"Send error on {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        return sent

    async def poll_and_forward(self):
        stats = await self._mem.stats()
        log.info(f"Poll | sources={len(self._sources)} | hashes={stats['tracked_hashes']} | posted_24h={stats['posted_last_24h']}")
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
            async for msg in self._client.iter_messages(channel, limit=50, min_id=last_id if last_id else 0, offset_date=cutoff, reverse=True):
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
        log.info(f"📨 {len(collected)} new message(s) from {channel}")
        for msg in collected:
            await self._handle_message(msg, channel)
            await asyncio.sleep(random.uniform(2, 6))
        await self._mem.set_last_msg_id(channel, new_last_id)

    async def _handle_message(self, msg, source_channel: str):
        text = msg.text or msg.message or ""
        text = _normalise_urls(text)

        image_data: Optional[bytes] = None
        image_mime = "image/jpeg"
        phash: Optional[str] = None

        if msg.media and _is_image(msg):
            try:
                buf = io.BytesIO()
                await self._client.download_media(msg.media, file=buf)
                image_data = buf.getvalue()
                image_mime = _doc_mime(msg)
                phash = self._mem.compute_phash(image_data)
                log.debug(f"Image: {len(image_data):,} bytes | mime={image_mime} | phash={phash}")
            except Exception as exc:
                log.warning(f"Image download failed: {exc}")

        # ForexFactory calendar route
        if image_data and (_looks_like_ff_image(text) or await self._image_looks_like_ff(image_data, image_mime)):
            is_weekly = _looks_like_weekly(text)
            await self._handle_ff_image(image_data, image_mime, text, is_weekly, source_channel, msg.id)
            return

        # Regular news with duplicate prevention
        content_hash = self._mem.hash_combined(text, image_data)

        if await self._mem.is_duplicate(content_hash):
            log.info(f"[SKIP] Exact hash duplicate — {content_hash[:12]}…")
            return

        if phash and await self._mem.is_image_duplicate(phash, max_distance=3):
            log.info(f"[SKIP] Image phash duplicate (Hamming ≤3) — {phash}")
            await self._mem.mark_image_seen(phash, source_channel)
            return

        if await self._is_similar_to_recent(text, image_data, phash):
            log.info(f"[SKIP] Duplicate detected (event name match or AI similarity).")
            await self._mem.mark_seen(content_hash, source=source_channel)
            if phash:
                await self._mem.mark_image_seen(phash, source_channel)
            return

        log.info(f"🔍 Analysing msg {msg.id} from {source_channel} | text={len(text)}c | image={'✅' if image_data else '❌'}")
        verdict = await self._ai.analyse(text, image_data, image_mime)
        await self._mem.mark_seen(content_hash, source=source_channel)
        if phash:
            await self._mem.mark_image_seen(phash, source_channel)

        if not verdict.get("approved"):
            log.info(f"[REJECTED] reason='{verdict.get('reason')}' | issues={verdict.get('issues')}")
            return

        post_text = verdict.get("formatted_text", "").strip()
        if not post_text:
            return

        delay = random.uniform(self._min_delay, self._max_delay)
        log.info(f"⏳ Waiting {delay:.1f}s before posting …")
        await asyncio.sleep(delay)
        await self._simulate_typing(len(post_text))

        sent = await self._broadcast_media(post_text, image_data, image_mime)
        if sent is None:
            return

        await self._mem.log_posted(source_channel, msg.id, sent.id, content_hash, verdict, post_text)
        await self._mem.store_recent_post(source_text=text[:1000], post_text=post_text[:1000], image_phash=phash)
        log.info(f"✅ Posted → msg_id={sent.id} | confidence={verdict.get('confidence')}")

    async def _is_similar_to_recent(self, new_text: str, new_image: Optional[bytes], new_phash: Optional[str] = None) -> bool:
        try:
            recent = await self._mem.get_recent_posts(limit=150)
            today_date = _eat_now().date()
            for old_text, old_phash, old_date_str in recent:
                old_date = datetime.fromisoformat(old_date_str).date()
                if new_phash and old_phash:
                    distance = self._mem.hamming_distance(new_phash, old_phash)
                    if distance <= 3:
                        log.info(f"Phash match: distance={distance}")
                        return True
                if old_text and new_text:
                    new_lower = new_text.lower()
                    old_lower = old_text.lower()
                    for name in _UNIQUE_EVENT_NAMES:
                        if name in new_lower and name in old_lower:
                            if old_date == today_date:
                                log.info(f"Matched critical event name '{name}' on same day – preventing duplicate")
                                return True
                            else:
                                log.info(f"Event name '{name}' matched but from different day – allowing")
                    same = await self._ai.is_same_story(
                        text_a=new_text[:500],
                        text_b=old_text[:500],
                        image_a=new_image,
                        image_b=None,
                    )
                    if same:
                        return True
        except Exception as exc:
            log.warning(f"Similarity check error: {exc}")
        return False

    async def _handle_ff_image(self, image_data: bytes, image_mime: str, caption: str,
                               is_weekly: bool, source_channel: str, msg_id: int):
        today_str = _eat_today_str()
        today_display = _eat_today_display()

        phash = self._mem.compute_phash(image_data)
        if phash and await self._mem.is_image_duplicate(phash, max_distance=3):
            log.info(f"[SKIP] FF image duplicate (phash distance ≤3) — {phash}")
            return

        if is_weekly:
            now = _eat_now()
            week_start = now + timedelta(days=(7 - now.weekday()))
            week_end = week_start + timedelta(days=4)
            week_range = f"{week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}"
            week_key = now.strftime("%Y-%W")

            if await self._mem.has_weekly_posted(week_key):
                log.info(f"[SKIP] Weekly calendar already posted this week ({week_key}).")
                return

            log.info("📆 Weekly FF image detected — analysing …")
            result = await self._ai.analyse_ff_image(image_data, image_mime, today_date=today_display,
                                                     is_weekly=True, week_range=week_range)
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
                if phash:
                    await self._mem.mark_image_seen(phash, source_channel)
                log.info(f"📆 Weekly calendar posted → msg_id={sent.id}")
        else:
            if await self._mem.has_daily_briefing(today_str):
                log.info(f"[SKIP] Daily briefing already posted today ({today_str}).")
                return

            log.info("📅 Daily FF image detected — analysing …")
            result = await self._ai.analyse_ff_image(image_data, image_mime, today_date=today_display, is_weekly=False)
            if not result.get("approved"):
                log.info(f"[SKIP] Daily FF image rejected: {result.get('reason')}")
                return
            post_text = result.get("formatted_text", "").strip()
            if not post_text:
                return
            post_text = _add_signature(post_text)

            events = _extract_events_from_ff_text(post_text)

            # --- Filter out geopolitical events from the calendar briefing ---
            filtered_events = []
            for e in events:
                name_lower = e.get("name", "").lower()
                if any(kw in name_lower for kw in GEOPOLITICAL_KEYWORDS):
                    log.info(f"Removing geopolitical event from calendar: {e.get('name')}")
                    continue
                filtered_events.append(e)
            events = filtered_events

            if not events:
                log.info("All events were geopolitical – skipping daily briefing.")
                return
            # ----------------------------------------------------------------

            self._todays_vip_events = self._select_vip_events(events)
            log.info(f"VIP reminder slots: {[e.get('name') for e in self._todays_vip_events]}")

            # Rebuild calendar text with filtered events (grouped)
            lines = post_text.split('\n')
            header = []
            footer = []
            in_events_section = False
            for line in lines:
                if line.startswith(("🔴", "🟠")):
                    in_events_section = True
                if not in_events_section and line.strip():
                    header.append(line)
                if line.startswith("Be careful") or line.startswith("#") or line.startswith("💡"):
                    footer.append(line)
            event_lines = []
            for e in events:
                emoji = "🔴" if e.get("impact") == "red" else "🟠"
                event_lines.append(f"{emoji} {e['time_12h']} | {e['currency']}: {e['name']}")
            new_calendar_text = "\n".join(header + event_lines + footer)
            post_text = new_calendar_text

            sent = await self._broadcast_file_with_caption(image_data, image_mime, post_text)
            if sent:
                await self._mem.save_daily_briefing(today_str, sent.id, events)
                if phash:
                    await self._mem.mark_image_seen(phash, source_channel)
                log.info(f"📅 Daily briefing posted → msg_id={sent.id}")

    async def _image_looks_like_ff(self, image_data: bytes, image_mime: str) -> bool:
        try:
            prompt = "Is this image a ForexFactory.com economic calendar screenshot? Respond with JSON: {\"is_ff\": true} or {\"is_ff\": false}"
            parts = [{"inline_data": {"mime_type": image_mime, "data": __import__('base64').b64encode(image_data).decode()}}, prompt]
            loop = asyncio.get_event_loop()
            resp = await asyncio.wait_for(loop.run_in_executor(None, lambda: self._ai._gemini_vision.generate_content(parts)), timeout=15)
            import json as _json, re as _re
            raw = resp.text
            raw = _re.sub(r"```+(?:json)?", "", raw).strip()
            data = _json.loads(raw)
            return bool(data.get("is_ff", False))
        except Exception:
            return False

    def _select_vip_events(self, events: List[dict]) -> List[dict]:
        eligible = [e for e in events if _is_reminder_eligible(e)]
        if not eligible:
            return []
        def sort_key(e):
            return (0 if _is_priority_event(e.get("name", "")) else 1, e.get("time_24h", "99:99"))
        vip = sorted(eligible, key=sort_key)[:1]   # only one reminder per day
        log.info(f"Top VIP (only one): {[e.get('name') for e in vip]}")
        return vip

    async def _check_reminders(self):
        today_str = _eat_today_str()
        reminder_count = await self._mem.get_reminder_count_today(today_str)
        log.info(f"Reminder status: {reminder_count}/1 sent today. Remaining slots: {1 - reminder_count}")
        if reminder_count >= 1:
            return
        briefing_msg_id = await self._mem.get_daily_briefing_msg_id(today_str)
        if not briefing_msg_id or briefing_msg_id == -1:
            return
        vip_events = self._todays_vip_events
        if not vip_events:
            try:
                async with self._mem._db.execute("SELECT events_json FROM daily_briefings WHERE date_str=?", (today_str,)) as cur:
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
        slots_left = 1 - reminder_count
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
                event_time = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {event_time_str}", "%Y-%m-%d %H:%M")
            except ValueError:
                continue
            minutes_until = (event_time - now_naive).total_seconds() / 60
            log.info(f"⏰ Pre-reminder: {event.get('name')} at {event.get('time_12h')} - {minutes_until:.0f} minutes from now")
            if 9 <= minutes_until <= 11:
                minutes_left = int(round(minutes_until))
                await self._send_reminder(event, event_key, briefing_msg_id, today_str, minutes_left)
                slots_left -= 1
                await asyncio.sleep(2)

    async def _send_reminder(self, event: dict, event_key: str, reply_to_msg_id: int, today_str: str, minutes_left: int):
        log.info(f"⏰ Sending {minutes_left}-min reminder for {event.get('name')} at {event.get('time_12h')}")
        event_name = event.get("name", "Unknown Event")
        impact_emoji = "🔴" if event.get("impact") == "red" else "🟠"
        mot_index = await self._mem.get_and_increment_motivational_index()
        motivational_line = await self._ai.get_motivational_line(event_name, mot_index)
        alert_text = (
            f"🚨 ALERT: {minutes_left} MINUTES REMAINING\n\n"
            f"{impact_emoji} {event_name}\n"
            f"🕒 {event.get('time_12h')}\n\n"
            f"REQUIRED ACTION:\n"
            f"✅ Secure open profits now\n"
            f"✅ Move Stop-Loss to Break-even\n"
            f"✅ No new entries during the release\n\n"
            f"{motivational_line}"
        )
        alert_text = _add_signature(alert_text)
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(dest, alert_text, parse_mode="md", reply_to=reply_to_msg_id)
                if sent:
                    log.info(f"🚨 Reminder sent to {dest} → msg_id={sent.id}")
            except Exception as exc:
                log.error(f"Reminder send failed to {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        await self._mem.mark_reminder_sent(event_key)
        await self._mem.increment_reminder_count(today_str)
        new_count = await self._mem.get_reminder_count_today(today_str)
        log.info(f"Reminder sent. Daily total now: {new_count}/1")

    async def _simulate_typing(self, text_len: int):
        duration = min(max(text_len / 180, 2), 14)
        if self._dest_channels:
            try:
                async with self._client.action(self._dest_channels[0], "typing"):
                    await asyncio.sleep(duration)
            except Exception:
                pass

    async def reminder_dispatcher_loop(self):
        log.info("🔔 Reminder dispatcher running …")
        while True:
            try:
                await self._check_reminders()
            except Exception as exc:
                log.error(f"Reminder dispatcher error: {exc}", exc_info=True)
            await asyncio.sleep(60)
