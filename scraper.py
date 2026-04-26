"""
scraper.py — Telethon scraper with real ForexFactory data + screenshot
Only posts if screenshot is successfully captured.
"""

import asyncio
import io
import json
import logging
import mimetypes
import os
import random
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple

import pytz
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

from ai_engine import AIEngine
from memory import MemoryManager
from forexfactory_xml import fetch_and_filter_events, STATIC_PROXIES

log = logging.getLogger("scraper")

EAT = pytz.timezone("Africa/Addis_Ababa")
_IMG_MIMES = {"image/jpeg", "image/png", "image/webp", "image/gif"}

# ─── CONFIGURATION ──────────────────────────────────────────────────────────
WEEKLY_OUTLOOK_HOUR = 22
WEEKLY_OUTLOOK_MINUTE = 10

TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
if TEST_MODE:
    log.warning("🧪 TEST_MODE enabled – weekly outlook will run NOW (real post if screenshot works)")

# ─── Helper functions ───────────────────────────────────────────────────────
def _is_image(msg) -> bool:
    if isinstance(msg.media, MessageMediaPhoto):
        return True
    if isinstance(msg.media, MessageMediaDocument):
        doc = msg.media.document
        return doc and doc.mime_type in _IMG_MIMES
    return False

def _doc_mime(msg) -> str:
    if isinstance(msg.media, MessageMediaDocument):
        return msg.media.document.mime_type or "image/jpeg"
    return "image/jpeg"

def _eat_now() -> datetime:
    return datetime.now(EAT)

def _eat_today_str() -> str:
    return _eat_now().strftime("%Y-%m-%d")

def _clean_caption(text: str) -> str:
    if not text:
        return ""
    return text.replace("EAT", "").replace("E.A.T", "").strip()[:1024]

# ─── Priority keywords and reminder logic ───────────────────────────────────
_PRIORITY_KEYWORDS = [
    "fomc", "federal open market committee", "interest rate decision",
    "nfp", "non-farm payroll", "cpi", "consumer price index",
    "pce", "core pce", "gdp", "fed chair", "powell speaks",
    "unemployment rate", "retail sales", "gold", "xau"
]
_FOMC_KEYWORDS = (
    "fomc", "federal open market committee", "federal reserve",
    "fed rate", "rate decision", "fed chair", "powell speaks",
    "fomc statement", "fomc minutes", "fomc press"
)

def _is_fomc_event(event_name: str) -> bool:
    return any(kw in event_name.lower() for kw in _FOMC_KEYWORDS)

def _is_reminder_eligible(event: dict) -> bool:
    if event.get("impact") != "red":
        return False
    if _is_fomc_event(event.get("name", "")):
        return True
    curr = event.get("currency", "").upper()
    name = event.get("name", "").lower()
    if curr != "USD" and not any(g in name for g in ("gold", "xau")):
        return False
    f = event.get("forecast", "").strip()
    p = event.get("previous", "").strip()
    if not (f and p and f != "—" and p != "—"):
        return False
    return True

def _is_priority_event(event_name: str) -> bool:
    return any(kw in event_name.lower() for kw in _PRIORITY_KEYWORDS)

# ─── ChannelScraper class ───────────────────────────────────────────────────
class ChannelScraper:
    def __init__(self, config: dict, ai_engine: AIEngine, memory: MemoryManager):
        self._cfg = config
        self._ai = ai_engine
        self._mem = memory
        self._test_mode = TEST_MODE

        self._dest_channels = config.get("dest_channels", [])
        if not self._dest_channels:
            single = config.get("dest_channel", "")
            if single:
                self._dest_channels = [single]
        if not self._dest_channels:
            raise ValueError("No destination channels configured.")
        log.info(f"📤 Post to {len(self._dest_channels)} channel(s) | test_mode={self._test_mode}")

        self._sources = config["source_channels"]
        self._min_delay = config["min_delay_seconds"]
        self._max_delay = config["max_delay_seconds"]
        self._lookback_hours = config["lookback_hours"]

        self._todays_events = []
        self._todays_vip_events = []
        self._daily_briefing_posted_date = None
        self._weekly_posted_date = None

        sess = config.get("session_string", "").strip()
        if sess:
            self._client = TelegramClient(StringSession(sess), config["api_id"], config["api_hash"])
        else:
            self._client = TelegramClient(config.get("session_name", "manager_session"), config["api_id"], config["api_hash"])

    async def start(self):
        sess = self._cfg.get("session_string", "").strip()
        if sess:
            await self._client.connect()
            if not await self._client.is_user_authorized():
                raise RuntimeError("StringSession invalid.")
        else:
            phone = self._cfg.get("phone", "")
            await self._client.start(phone=phone if phone else None)
        me = await self._client.get_me()
        log.info(f"✅ Logged as: {me.first_name} (@{me.username or me.id})")

    async def stop(self):
        await self._client.disconnect()

    async def _ensure_connected(self) -> bool:
        if not self._client.is_connected():
            log.warning("Disconnected, reconnecting...")
            try:
                await self._client.connect()
                if not await self._client.is_user_authorized():
                    return False
                log.info("✅ Reconnected.")
            except Exception as e:
                log.error(f"Reconnect fail: {e}")
                return False
        return True

    # ─── Broadcast (real posting) ───────────────────────────────────────────
    async def _broadcast_text(self, text: str):
        sent = None
        safe = _clean_caption(text)
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(dest, safe, parse_mode="md")
                log.info(f"  → Text to {dest} | id={sent.id}")
            except FloodWaitError as fw:
                await asyncio.sleep(fw.seconds + 3)
                sent = await self._client.send_message(dest, safe, parse_mode="md")
            except Exception as e:
                log.error(f"Send text error {dest}: {e}")
            await asyncio.sleep(1)
        return sent

    async def _broadcast_file_with_caption(self, data: bytes, mime: str, caption: str):
        sent = None
        safe_cap = _clean_caption(caption)
        for dest in self._dest_channels:
            try:
                ext = mimetypes.guess_extension(mime) or ".png"
                buf = io.BytesIO(data)
                buf.name = f"cal{ext}"
                buf.seek(0)
                sent = await self._client.send_file(dest, buf, caption=safe_cap, parse_mode="md", force_document=False)
                log.info(f"  → File to {dest} | id={sent.id}")
            except Exception as e:
                log.error(f"Send file error {dest}: {e}")
                sent = await self._client.send_message(dest, safe_cap, parse_mode="md")
            await asyncio.sleep(1)
        return sent

    # ─── Data fetching (real XML) ───────────────────────────────────────────
    async def _scrape_forex_factory_week(self) -> List[dict]:
        try:
            loop = asyncio.get_event_loop()
            events = await loop.run_in_executor(None, lambda: fetch_and_filter_events("USD", "High"))
            if events:
                log.info(f"✅ Real XML: {len(events)} USD High events")
                return events
            else:
                log.warning("XML returned no events")
                return []
        except Exception as e:
            log.error(f"XML fetch failed: {e}")
            return []

    # ─── Screenshot with retry (returns None if fails) ──────────────────────
    async def _take_forex_factory_screenshot_week(self, retries: int = 3) -> Optional[bytes]:
        """Take screenshot with retry logic. Returns None if all retries fail."""
        from playwright.async_api import async_playwright
        
        # Disable proxy for reliability; change to a proxy if needed
        proxy_url = None   # or random.choice(STATIC_PROXIES)
        proxy_config = {"server": proxy_url} if proxy_url else None
        
        for attempt in range(retries):
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(
                        headless=True,
                        args=["--no-sandbox", "--disable-setuid-sandbox"],
                        proxy=proxy_config
                    )
                    context = await browser.new_context(
                        locale="en-US",
                        timezone_id="Africa/Addis_Ababa",
                        viewport={"width": 1280, "height": 1800}
                    )
                    page = await context.new_page()
                    
                    await page.goto("https://www.forexfactory.com/calendar", timeout=60000)
                    try:
                        await page.wait_for_selector(".calendar__row", timeout=30000)
                    except:
                        await page.wait_for_timeout(5000)
                    
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(3000)
                    
                    # Apply USD + High filter
                    await page.evaluate("""
                        document.querySelectorAll('.calendar__row--event').forEach(row => {
                            const currEl = row.querySelector('.calendar__currency');
                            const impEl = row.querySelector('.calendar__impact span');
                            let hide = false;
                            if (currEl && currEl.innerText.trim().toUpperCase() !== 'USD') hide = true;
                            if (impEl) {
                                const cls = impEl.className;
                                const txt = impEl.innerText.trim().toLowerCase();
                                if (!cls.includes('high') && !cls.includes('impact--high') && txt !== 'high') hide = true;
                            } else hide = true;
                            if (hide) row.style.display = 'none';
                        });
                        document.querySelectorAll('.calendar__row--day-breaker').forEach(r => r.style.display = 'none');
                    """)
                    
                    table = await page.query_selector(".calendar__table")
                    if table:
                        screenshot = await table.screenshot(type="png")
                    else:
                        screenshot = await page.screenshot(full_page=True, type="png")
                    
                    await browser.close()
                    log.info(f"Screenshot captured (attempt {attempt+1}): {len(screenshot)} bytes")
                    return screenshot
            except Exception as e:
                log.warning(f"Screenshot attempt {attempt+1} failed: {e}")
                await asyncio.sleep(3)
        log.error("All screenshot attempts failed")
        return None

    # ─── Weekly outlook: only post if screenshot succeeds ───────────────────
    async def _check_weekly_outlook(self):
        now = _eat_now()
        if not self._test_mode:
            if now.weekday() != 6 or now.hour != WEEKLY_OUTLOOK_HOUR or now.minute != WEEKLY_OUTLOOK_MINUTE:
                return
            log.info("Scheduled weekly outlook")
        else:
            log.info("TEST_MODE: running weekly outlook immediately")

        week_key = now.strftime("%Y-%W")
        if self._weekly_posted_date == week_key and not self._test_mode:
            return

        events = await self._scrape_forex_factory_week()
        if not events:
            log.error("No events from real source → aborting")
            return

        week_start = now + timedelta(days=1)
        week_end = week_start + timedelta(days=4)
        week_range = f"{week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}"
        caption = await self._ai.generate_weekly_outlook(events, week_range)
        if not caption:
            caption = self._fallback_weekly(events, week_range)

        screenshot = await self._take_forex_factory_screenshot_week(retries=3)
        if not screenshot:
            log.error("Screenshot failed after retries → no post will be sent")
            return

        sent = await self._broadcast_file_with_caption(screenshot, "image/png", caption)
        if sent:
            self._weekly_posted_date = week_key
            log.info(f"Weekly outlook posted with screenshot, msg_id={sent.id}")
        else:
            log.error("Failed to send post")

    @staticmethod
    def _fallback_weekly(events: list, week_range: str) -> str:
        from itertools import groupby
        lines = [f"📅 WEEKLY HIGH IMPACT\nWeek of {week_range}\n"]
        for day, grp in groupby(events, key=lambda e: e.get("date", "Unknown")):
            lines.append(f"\n{day}")
            for ev in grp:
                fcast = ev.get("forecast", "").strip()
                prev = ev.get("previous", "").strip()
                detail = ""
                if fcast and fcast != "—" and prev and prev != "—":
                    detail = f"  ↳ Forecast: {fcast} | Previous: {prev}"
                elif fcast and fcast != "—":
                    detail = f"  ↳ Forecast: {fcast}"
                elif prev and prev != "—":
                    detail = f"  ↳ Previous: {prev}"
                lines.append(f"🔴 {ev.get('time_12h', '—')} | USD: {ev.get('name', 'Unknown')}")
                if detail:
                    lines.append(detail)
        lines.append("\n📌 NOTE:\nMonitor all USD Red events closely. Manage risk carefully.")
        return "\n".join(lines)

    # ─── Daily briefing (same logic – only post if screenshot works) ────────
    async def _check_daily_briefing(self):
        now = _eat_now()
        today = _eat_today_str()
        if not (7 <= now.hour < 9):
            return
        if await self._mem.has_daily_briefing(today):
            return
        log.info(f"Daily briefing {today}")
        events = await self._scrape_forex_factory_today()
        if not events:
            await self._mem.save_daily_briefing(today, -1, [])
            return
        self._todays_events = events
        self._todays_vip_events = self._select_vip_events(events)
        date_display = now.strftime("%A, %B %d, %Y")
        caption = await self._ai.generate_daily_briefing(events, date_display)
        if not caption:
            return
        screenshot = await self._take_forex_factory_screenshot_today()
        if not screenshot:
            log.error("Daily briefing screenshot failed – no post")
            return
        sent = await self._broadcast_file_with_caption(screenshot, "image/png", caption)
        if sent:
            await self._mem.save_daily_briefing(today, sent.id, events)

    async def _scrape_forex_factory_today(self) -> List[dict]:
        all_week = await self._scrape_forex_factory_week()
        today_str = _eat_now().strftime("%Y-%m-%d")
        return [e for e in all_week if e.get("date") == today_str]

    async def _take_forex_factory_screenshot_today(self) -> Optional[bytes]:
        from playwright.async_api import async_playwright
        proxy_url = None
        proxy_config = {"server": proxy_url} if proxy_url else None
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"], proxy=proxy_config)
                context = await browser.new_context(locale="en-US", timezone_id="Africa/Addis_Ababa", viewport={"width": 1280, "height": 1000})
                page = await context.new_page()
                await page.goto("https://www.forexfactory.com/calendar?day=today", timeout=60000)
                await page.wait_for_load_state("networkidle", timeout=60000)
                await page.wait_for_selector(".calendar__row", timeout=30000)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(2000)
                await page.evaluate("""
                    document.querySelectorAll('.calendar__row--event').forEach(row => {
                        const currEl = row.querySelector('.calendar__currency');
                        const impEl = row.querySelector('.calendar__impact span');
                        let hide = false;
                        if (currEl && currEl.innerText.trim().toUpperCase() !== 'USD') hide = true;
                        if (impEl) {
                            const cls = impEl.className;
                            const txt = impEl.innerText.trim().toLowerCase();
                            if (!cls.includes('high') && !cls.includes('impact--high') && txt !== 'high') hide = true;
                        } else hide = true;
                        if (hide) row.style.display = 'none';
                    });
                    document.querySelectorAll('.calendar__row--day-breaker').forEach(r => r.style.display = 'none');
                """)
                table = await page.query_selector(".calendar__table")
                if table:
                    screenshot = await table.screenshot(type="png")
                else:
                    screenshot = await page.screenshot(full_page=True, type="png")
                await browser.close()
                return screenshot
        except Exception as e:
            log.error(f"Daily screenshot failed: {e}")
            return None

    # ─── VIP selection (unchanged) ─────────────────────────────────────────
    def _select_vip_events(self, events: List[dict]) -> List[dict]:
        eligible = [e for e in events if _is_reminder_eligible(e)]
        if not eligible:
            return []
        def score(e):
            name = e.get("name", "").lower()
            if any(x in name for x in ("powell","fomc","rate decision")):
                return 0
            return 1 if _is_priority_event(name) else 2
        day = [e for e in eligible if e.get("time_24h","99:99") < "18:00"]
        night = [e for e in eligible if e.get("time_24h","99:99") >= "18:00"]
        vip = []
        if day:
            day.sort(key=lambda e: (score(e), e.get("time_24h")))
            vip.append(day[0])
        if night:
            night.sort(key=lambda e: (score(e), e.get("time_24h")))
            vip.append(night[0])
        vip.sort(key=lambda e: e.get("time_24h"))
        return vip

    # ─── Reminders (unchanged) ──────────────────────────────────────────────
    async def _check_reminders(self):
        today = _eat_today_str()
        cnt = await self._mem.get_reminder_count_today(today)
        if cnt >= 2:
            return
        brief_id = await self._mem.get_daily_briefing_msg_id(today)
        if not brief_id or brief_id == -1:
            return
        vip = self._todays_vip_events
        if not vip:
            async with self._mem._db.execute("SELECT events_json FROM daily_briefings WHERE date_str=?", (today,)) as cur:
                row = await cur.fetchone()
            if row and row["events_json"]:
                vip = self._select_vip_events(json.loads(row["events_json"]))
                self._todays_vip_events = vip
            if not vip:
                return
        now = _eat_now().replace(tzinfo=None)
        slots = 2 - cnt
        for ev in vip:
            if slots <= 0:
                break
            key = f"{today}_{ev.get('name')}_{ev.get('currency')}"
            if await self._mem.has_reminder_been_sent(key):
                continue
            t24 = ev.get("time_24h")
            if not t24:
                continue
            try:
                ev_time = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t24}", "%Y-%m-%d %H:%M")
            except:
                continue
            mins = (ev_time - now).total_seconds() / 60
            if 8 <= mins <= 12:
                await self._send_reminder(ev, key, brief_id, today)
                slots -= 1
                await asyncio.sleep(2)

    async def _send_reminder(self, event: dict, key: str, reply_id: int, today: str):
        log.info(f"⏰ Reminder for {event.get('name')}")
        mot = await self._mem.get_and_increment_motivational_index()
        alert = await self._ai.generate_alert(event, motivational_index=mot)
        if not alert:
            return
        safe = _clean_caption(alert)
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(dest, safe, parse_mode="md", reply_to=reply_id)
                log.info(f"Reminder to {dest} id={sent.id}")
            except Exception as e:
                log.error(f"Reminder fail {dest}: {e}")
            await asyncio.sleep(1)
        await self._mem.mark_reminder_sent(key)
        await self._mem.increment_reminder_count(today)

    # ─── Channel processing (unchanged) ─────────────────────────────────────
    async def poll_and_forward(self):
        stats = await self._mem.stats()
        log.info(f"Poll cycle | sources={len(self._sources)} | hashes={stats['tracked_hashes']} | posted_24h={stats['posted_last_24h']}")
        if not await self._ensure_connected():
            return
        await self._check_daily_briefing()
        await self._check_reminders()
        await self._check_weekly_outlook()
        for ch in self._sources:
            try:
                await self._process_channel(ch)
            except FloodWaitError as fw:
                await asyncio.sleep(fw.seconds + 5)
            except Exception as e:
                log.error(f"Error on {ch}: {e}")
                await asyncio.sleep(5)

    async def _process_channel(self, channel: str):
        if not await self._ensure_connected():
            return
        last = await self._mem.get_last_msg_id(channel)
        cutoff = None
        if last == 0:
            cutoff = datetime.now(pytz.UTC) - timedelta(hours=self._lookback_hours)
        new_last = last
        collected = []
        try:
            async for msg in self._client.iter_messages(channel, limit=50, min_id=last if last else 0, offset_date=cutoff, reverse=True):
                if msg.id <= last:
                    continue
                if not (msg.text or msg.media):
                    continue
                collected.append(msg)
                new_last = max(new_last, msg.id)
        except Exception as e:
            log.error(f"iter error {channel}: {e}")
            await self._ensure_connected()
            return
        if not collected:
            await self._mem.set_last_msg_id(channel, new_last)
            return
        log.info(f"📨 {len(collected)} new from {channel}")
        for msg in collected:
            await self._handle_message(msg, channel)
            await asyncio.sleep(random.uniform(2, 6))
        await self._mem.set_last_msg_id(channel, new_last)

    async def _handle_message(self, msg, src: str):
        text = msg.text or ""
        img = None
        mime = "image/jpeg"
        if msg.media and _is_image(msg):
            try:
                buf = io.BytesIO()
                await self._client.download_media(msg.media, file=buf)
                img = buf.getvalue()
                mime = _doc_mime(msg)
            except Exception as e:
                log.warning(f"Image download fail: {e}")

        thash = self._mem.hash_combined(text, None) if text else None
        ihash = self._mem.hash_combined("", img) if img else None
        if (thash and await self._mem.is_duplicate(thash)) or (ihash and await self._mem.is_duplicate(ihash)):
            log.info("[SKIP] duplicate")
            return
        if thash:
            await self._mem.mark_seen(thash, source=src)
        if ihash:
            await self._mem.mark_seen(ihash, source=src)

        log.info(f"🔍 Analysing msg {msg.id} from {src} | text={len(text)} | img={'✅' if img else '❌'}")
        verdict = await self._ai.analyse(text, img, mime)
        chash = self._mem.hash_combined(text, img)

        if not verdict.get("approved"):
            log.info(f"[REJECT] {verdict.get('reason')}")
            return

        post = self._build_post(verdict)
        delay = random.uniform(self._min_delay, self._max_delay)
        log.info(f"⏳ wait {delay:.1f}s")
        await asyncio.sleep(delay)
        await self._simulate_typing(len(post))

        sent = await self._broadcast_media(post, img, mime)
        if sent:
            await self._mem.log_posted(src, msg.id, sent.id, chash, verdict, post)
            log.info(f"✅ Posted id={sent.id} engine={verdict.get('engine')}")

    async def _broadcast_media(self, text: str, img: Optional[bytes], mime: str):
        """Legacy method – kept for channel forwarding (text with optional image)"""
        sent = None
        safe = _clean_caption(text)
        for dest in self._dest_channels:
            try:
                if img:
                    ext = mimetypes.guess_extension(mime) or ".jpg"
                    buf = io.BytesIO(img)
                    buf.name = f"media{ext}"
                    buf.seek(0)
                    sent = await self._client.send_file(dest, buf, caption=safe, parse_mode="md")
                else:
                    sent = await self._client.send_message(dest, safe, parse_mode="md")
                log.info(f"  → Post to {dest} | id={sent.id}")
            except FloodWaitError as fw:
                await asyncio.sleep(fw.seconds + 3)
                sent = await self._client.send_message(dest, safe, parse_mode="md")
            except Exception as e:
                log.error(f"Send error {dest}: {e}")
            await asyncio.sleep(1)
        return sent

    @staticmethod
    def _build_post(v: dict) -> str:
        body = v.get("formatted_text", "").strip()
        tags = v.get("hashtags", "").strip()
        if tags and not body.endswith(tags):
            body = f"{body}\n\n{tags}"
        if random.random() < 0.3:
            body += "\n\n💡 [Squad 4xx](https://t.me/Squad_4xx)"
        return body

    async def _simulate_typing(self, length: int):
        dur = min(max(length / 180, 2), 14)
        if self._dest_channels:
            try:
                async with self._client.action(self._dest_channels[0], "typing"):
                    await asyncio.sleep(dur)
            except:
                pass

    async def reminder_dispatcher_loop(self):
        log.info("🔔 Reminder loop started")
        while True:
            try:
                await self._check_reminders()
            except Exception as e:
                log.error(f"Reminder error: {e}")
            await asyncio.sleep(60)
