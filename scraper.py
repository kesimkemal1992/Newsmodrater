"""
scraper.py — Telethon channel scraper, forwarder, and Forex Factory scheduler.

Fixed issues:
  - Increased Playwright timeout to 30 seconds + extra wait (fixes missing FOMC/GDP events)
  - Improved high-impact detection (supports "high" and "impact--high" classes)
  - Added detailed logging for weekly outlook (shows red event count)
  - Configurable weekly outlook hour (default 22 for testing, changeable)
"""

import asyncio
import io
import json
import logging
import mimetypes
import random
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

import pytz
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

from ai_engine import AIEngine
from memory import MemoryManager

log = logging.getLogger("scraper")

EAT = pytz.timezone("Africa/Addis_Ababa")

_IMG_MIMES = {"image/jpeg", "image/png", "image/webp", "image/gif"}

# ── CONFIGURATION ────────────────────────────────────────────────────────────
# Weekly outlook schedule (hour in EAT, 0-23). Default is 22 (10 PM) for testing.
# Change to 21 (9 PM) for production.
WEEKLY_OUTLOOK_HOUR = 22   # 10:00 PM EAT
WEEKLY_OUTLOOK_MINUTE = 48  # Currently only hour is checked; to test at 22:46, set hour=22 and minute check below

# Playwright timeout in milliseconds
PLAYWRIGHT_TIMEOUT_MS = 30000  # 30 seconds
PLAYWRIGHT_EXTRA_WAIT_MS = 3000  # 3 seconds extra

# ── Reminder eligibility rules ─────────────────────────────────────────────────
_PRIORITY_KEYWORDS = [
    "fomc", "federal open market committee",
    "interest rate decision", "rate decision",
    "nfp", "non-farm payroll", "non-farm payrolls",
    "cpi", "consumer price index",
    "pce", "core pce",
    "gdp",
    "fed chair", "powell speaks", "jerome powell",
    "unemployment rate",
    "retail sales",
    "gold", "xau",
]

_FOMC_KEYWORDS = (
    "fomc",
    "federal open market committee",
    "federal reserve",
    "fed rate",
    "rate decision",
    "interest rate decision",
    "fed chair",
    "powell speaks",
    "jerome powell",
    "fomc statement",
    "fomc minutes",
    "fomc meeting",
    "fomc press",
)

_REMINDER_CURRENCIES = {"USD"}
_GOLD_KEYWORDS = ("gold", "xau")


def _is_fomc_event(event_name: str) -> bool:
    name_lower = event_name.lower()
    return any(kw in name_lower for kw in _FOMC_KEYWORDS)


def _is_reminder_eligible(event: dict) -> bool:
    currency = event.get("currency", "").upper().strip()
    name_lower = event.get("name", "").lower()
    if event.get("impact") != "red":
        return False
    if _is_fomc_event(event.get("name", "")):
        return True
    is_usd = currency == "USD"
    is_gold = any(kw in name_lower for kw in _GOLD_KEYWORDS)
    if not (is_usd or is_gold):
        return False
    forecast = event.get("forecast", "").strip()
    previous = event.get("previous", "").strip()
    has_forecast = bool(forecast and forecast != "—")
    has_previous = bool(previous and previous != "—")
    if not (has_forecast and has_previous):
        return False
    return True


def _is_priority_event(event_name: str) -> bool:
    name_lower = event_name.lower()
    return any(kw in name_lower for kw in _PRIORITY_KEYWORDS)


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


def _clean_caption(text: str) -> str:
    if not text:
        return ""
    text = text.replace("EAT", "").replace("E.A.T", "").strip()
    return text[:1024]


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
            raise ValueError(
                "No destination channels configured. "
                "Set DEST_CHANNELS (comma-separated) or DEST_CHANNEL in environment."
            )

        log.info(f"📤  Posting to {len(self._dest_channels)} destination channel(s): {self._dest_channels}")

        self._sources = config["source_channels"]
        self._min_delay = config["min_delay_seconds"]
        self._max_delay = config["max_delay_seconds"]
        self._lookback_hours = config["lookback_hours"]

        self._todays_events: List[dict] = []
        self._todays_vip_events: List[dict] = []
        self._daily_briefing_posted_date: Optional[str] = None
        self._weekly_posted_date: Optional[str] = None

        session_string = config.get("session_string", "").strip()
        if session_string:
            session = StringSession(session_string)
            log.info("Using StringSession for authentication.")
        else:
            session = config.get("session_name", "manager_session")
            log.info(f"Using file session: {session}.session")

        self._client = TelegramClient(session, config["api_id"], config["api_hash"])

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

    async def stop(self):
        await self._client.disconnect()

    async def _ensure_connected(self) -> bool:
        if not self._client.is_connected():
            log.warning("Telethon disconnected — reconnecting …")
            try:
                await self._client.connect()
                if not await self._client.is_user_authorized():
                    log.error("Session expired after reconnect.")
                    return False
                log.info("✅  Reconnected successfully.")
            except Exception as exc:
                log.error(f"Reconnect failed: {exc}")
                return False
        return True

    async def _broadcast_text(self, text: str):
        sent = None
        safe_text = _clean_caption(text)
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(dest, safe_text, parse_mode="md")
                log.info(f"  → Text sent to {dest} | msg_id={sent.id}")
            except ChatWriteForbiddenError:
                log.error(f"❌  Cannot post to {dest} — check admin rights.")
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s on {dest} — sleeping …")
                await asyncio.sleep(fwe.seconds + 3)
                try:
                    sent = await self._client.send_message(dest, safe_text, parse_mode="md")
                except Exception as exc:
                    log.error(f"Retry failed for {dest}: {exc}")
            except Exception as exc:
                log.error(f"Send text error on {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        return sent

    async def _broadcast_file_with_caption(self, file_bytes: bytes, mime: str, caption: str):
        sent = None
        safe_caption = _clean_caption(caption)
        for dest in self._dest_channels:
            try:
                ext = mimetypes.guess_extension(mime) or ".png"
                buf = io.BytesIO(file_bytes)
                buf.name = f"calendar{ext}"
                buf.seek(0)
                sent = await self._client.send_file(dest, buf, caption=safe_caption, parse_mode="md", force_document=False)
                log.info(f"  → File sent to {dest} | msg_id={sent.id}")
            except Exception as exc:
                log.error(f"Send file error on {dest}: {exc}", exc_info=True)
                log.info(f"Falling back to text-only for {dest}.")
                try:
                    sent = await self._client.send_message(dest, safe_caption, parse_mode="md")
                except Exception as exc2:
                    log.error(f"Text fallback also failed for {dest}: {exc2}")
            await asyncio.sleep(1)
        return sent

    async def _broadcast_media(self, text: str, image_data: Optional[bytes], image_mime: str):
        sent = None
        safe_text = _clean_caption(text)
        for dest in self._dest_channels:
            try:
                if image_data:
                    buf = io.BytesIO(image_data)
                    ext = mimetypes.guess_extension(image_mime) or ".jpg"
                    buf.name = f"media{ext}"
                    buf.seek(0)
                    sent = await self._client.send_file(dest, buf, caption=safe_text, parse_mode="md")
                else:
                    sent = await self._client.send_message(dest, safe_text, parse_mode="md")
                log.info(f"  → Post sent to {dest} | msg_id={sent.id}")
            except ChatWriteForbiddenError:
                log.error(f"❌  Cannot post to {dest} — check admin rights.")
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s on {dest} — retrying …")
                await asyncio.sleep(fwe.seconds + 3)
                try:
                    sent = await self._client.send_message(dest, safe_text, parse_mode="md")
                except Exception as exc:
                    log.error(f"Retry failed for {dest}: {exc}")
            except Exception as exc:
                log.error(f"Send error on {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        return sent

    async def poll_and_forward(self):
        stats = await self._mem.stats()
        log.info(
            f"Poll cycle | sources={len(self._sources)} | "
            f"hashes={stats['tracked_hashes']} | "
            f"posted_24h={stats['posted_last_24h']} | "
            f"reminders_today={stats.get('pending_reminders', 0)}"
        )
        if not await self._ensure_connected():
            log.warning("Skipping poll cycle — not connected.")
            return
        await self._check_daily_briefing()
        await self._check_reminders()
        await self._check_weekly_outlook()
        for channel in self._sources:
            try:
                await self._process_channel(channel)
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s — sleeping …")
                await asyncio.sleep(fwe.seconds + 5)
            except Exception as exc:
                log.error(f"Error on channel {channel}: {exc}", exc_info=True)
                await asyncio.sleep(5)

    async def _check_daily_briefing(self):
        now = _eat_now()
        today_str = _eat_today_str()
        if not (7 <= now.hour < 9):
            return
        if await self._mem.has_daily_briefing(today_str):
            return
        log.info(f"📅  Daily briefing time! Scraping ForexFactory for {today_str} …")
        events = await self._scrape_forex_factory_today()
        if not events:
            log.info("No high-impact events today — skipping daily briefing.")
            await self._mem.save_daily_briefing(today_str, -1, [])
            return
        self._todays_events = events
        self._todays_vip_events = self._select_vip_events(events)
        log.info(f"VIP reminder slots reserved for: {[e.get('name') for e in self._todays_vip_events]}")
        date_display = now.strftime("%A, %B %d, %Y")
        briefing_text = await self._ai.generate_daily_briefing(events, date_display)
        if not briefing_text:
            log.error("Failed to generate daily briefing text.")
            return
        log.info("📸  Taking today's ForexFactory screenshot …")
        screenshot = await self._take_forex_factory_screenshot_today()
        if screenshot:
            sent = await self._broadcast_file_with_caption(screenshot, "image/png", briefing_text)
        else:
            log.warning("Today screenshot failed — sending text only.")
            sent = await self._broadcast_text(briefing_text)
        if sent:
            await self._mem.save_daily_briefing(today_str, sent.id, events)
            log.info(f"📅  Daily briefing posted → msg_id={sent.id}")
        else:
            log.error("Failed to send daily briefing.")

    def _select_vip_events(self, events: List[dict]) -> List[dict]:
        eligible = [e for e in events if _is_reminder_eligible(e)]
        if not eligible:
            log.info("No reminder-eligible events today.")
            return []

        def get_score(event):
            name = event.get("name", "").lower()
            if "powell" in name or "fomc" in name or "rate decision" in name:
                return 0
            if any(k in name for k in _PRIORITY_KEYWORDS):
                return 1
            return 2

        day_events = [e for e in eligible if e.get("time_24h", "99:99") < "18:00"]
        night_events = [e for e in eligible if e.get("time_24h", "99:99") >= "18:00"]

        vip = []
        if day_events:
            day_events.sort(key=lambda e: (get_score(e), e.get("time_24h", "99:99")))
            vip.append(day_events[0])
        if night_events:
            night_events.sort(key=lambda e: (get_score(e), e.get("time_24h", "99:99")))
            vip.append(night_events[0])
        vip.sort(key=lambda e: e.get("time_24h", "99:99"))
        log.info(f"VIP slots reserved for {len(vip)} event(s): {[e.get('name') for e in vip]}")
        return vip

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
            async with self._mem._db.execute(
                "SELECT events_json FROM daily_briefings WHERE date_str=?", (today_str,)
            ) as cur:
                row = await cur.fetchone()
            if row and row["events_json"]:
                all_events = json.loads(row["events_json"])
                vip_events = self._select_vip_events(all_events)
                self._todays_vip_events = vip_events
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
                event_time = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {event_time_str}", "%Y-%m-%d %H:%M")
            except ValueError:
                continue
            minutes_until = (event_time - now_naive).total_seconds() / 60
            if 8 <= minutes_until <= 12:
                await self._send_reminder(event, event_key, briefing_msg_id, today_str)
                slots_left -= 1
                await asyncio.sleep(2)

    async def _send_reminder(self, event: dict, event_key: str, reply_to_msg_id: int, today_str: str):
        log.info(f"⏰  Sending 10-min reminder for: {event.get('name')}")
        mot_index = await self._mem.get_and_increment_motivational_index()
        alert_text = await self._ai.generate_alert(event, motivational_index=mot_index)
        if not alert_text:
            log.error(f"Failed to generate alert for {event.get('name')}")
            return
        safe_alert = _clean_caption(alert_text)
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(
                    dest, safe_alert, parse_mode="md", reply_to=reply_to_msg_id
                )
                if sent:
                    log.info(f"🚨  Reminder sent to {dest} → msg_id={sent.id}")
            except Exception as exc:
                log.error(f"Failed to send reminder to {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        await self._mem.mark_reminder_sent(event_key)
        await self._mem.increment_reminder_count(today_str)

    # ─── WEEKLY OUTLOOK WITH FIXES (timeout, logging, configurable hour) ───
    async def _check_weekly_outlook(self):
        now = _eat_now()
        # Sunday? (weekday 6 = Sunday)
        if now.weekday() != 6:
            return
        # Check hour (and optionally minute for precise testing)
        if now.hour != WEEKLY_OUTLOOK_HOUR:
            return
        # Optional minute check (uncomment to test at specific minute)
        # if now.minute != WEEKLY_OUTLOOK_MINUTE:
        #     return
        
        week_key = now.strftime("%Y-%W")
        if self._weekly_posted_date == week_key:
            log.debug(f"Weekly outlook already posted for week {week_key}")
            return
        
        log.info("📆  Sunday weekly outlook time! Scraping ForexFactory for the week …")
        events = await self._scrape_forex_factory_week()
        if not events:
            log.info("No high-impact events this week — skipping weekly outlook.")
            self._weekly_posted_date = week_key
            return
        
        week_start = now + timedelta(days=1)
        week_end = week_start + timedelta(days=4)
        week_range = f"{week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}"
        outlook_text = await self._ai.generate_weekly_outlook(events, week_range)
        if not outlook_text:
            log.error("Failed to generate weekly outlook.")
            return
        
        log.info("📸  Taking weekly ForexFactory screenshot …")
        screenshot = await self._take_forex_factory_screenshot_week()
        if screenshot:
            sent = await self._broadcast_file_with_caption(screenshot, "image/png", outlook_text)
        else:
            log.warning("Weekly screenshot failed — sending text only.")
            sent = await self._broadcast_text(outlook_text)
        if sent:
            self._weekly_posted_date = week_key
            log.info(f"📆  Weekly outlook posted → msg_id={sent.id}")

    # ─── ForexFactory Playwright wrappers with increased timeout ───────────
    async def _scrape_forex_factory_today(self) -> List[dict]:
        return await self._playwright_scrape_today()

    async def _scrape_forex_factory_week(self) -> List[dict]:
        return await self._playwright_scrape_week()

    async def _take_forex_factory_screenshot_today(self) -> Optional[bytes]:
        return await self._playwright_screenshot_today()

    async def _take_forex_factory_screenshot_week(self) -> Optional[bytes]:
        return await self._playwright_screenshot_week()

    # ─── Playwright: Today Scrape with fixes ───────────────────────────────
    async def _playwright_scrape_today(self) -> List[dict]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"])
                context = await browser.new_context(locale="en-US", timezone_id="Africa/Addis_Ababa", user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                page = await context.new_page()
                await page.goto("https://www.forexfactory.com/calendar?day=today", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=PLAYWRIGHT_TIMEOUT_MS)
                # Extra wait for dynamic content
                await page.wait_for_timeout(PLAYWRIGHT_EXTRA_WAIT_MS)
                events = await self._extract_events_from_page(page)
                await browser.close()
                red_events = [e for e in events if e.get("impact") == "red"]
                log.info(f"Today scrape: {len(events)} total high-impact events, {len(red_events)} red (USD only?)")
                return events
        except Exception as exc:
            log.error(f"Playwright today scrape failed: {exc}", exc_info=True)
            return []

    async def _playwright_scrape_week(self) -> List[dict]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"])
                context = await browser.new_context(locale="en-US", timezone_id="Africa/Addis_Ababa", user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                page = await context.new_page()
                await page.goto("https://www.forexfactory.com/calendar", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=PLAYWRIGHT_TIMEOUT_MS)
                await page.wait_for_timeout(PLAYWRIGHT_EXTRA_WAIT_MS)
                events = await self._extract_events_from_page(page)
                await browser.close()
                red_events = [e for e in events if e.get("impact") == "red"]
                log.info(f"Week scrape: {len(events)} total high-impact events, {len(red_events)} red (USD only?)")
                return events
        except Exception as exc:
            log.error(f"Playwright week scrape failed: {exc}", exc_info=True)
            return []

    async def _playwright_screenshot_today(self) -> Optional[bytes]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"])
                context = await browser.new_context(locale="en-US", timezone_id="Africa/Addis_Ababa", viewport={"width": 1280, "height": 1000}, user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                page = await context.new_page()
                await page.goto("https://www.forexfactory.com/calendar?day=today", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=PLAYWRIGHT_TIMEOUT_MS)
                await page.wait_for_timeout(PLAYWRIGHT_EXTRA_WAIT_MS)
                await page.evaluate("""
                    document.querySelectorAll('.calendar__row').forEach(row => {
                        const imp = row.querySelector('.calendar__impact span');
                        const curr = row.querySelector('.calendar__currency');
                        let hide = false;
                        if (imp) {
                            const cls = imp.className;
                            if (!cls.includes('high') && !cls.includes('medium')) { hide = true; }
                        }
                        if (curr && curr.innerText.trim().toUpperCase() !== 'USD') {
                            hide = true;
                        }
                        if (hide) { row.style.display = 'none'; }
                    });
                    document.querySelectorAll('.calendar__row--day-breaker').forEach(r => { r.style.display = 'none'; });
                """)
                table = await page.query_selector(".calendar__table")
                screenshot_bytes = await table.screenshot(type="png") if table else await page.screenshot(clip={"x": 0, "y": 0, "width": 1280, "height": 1000}, type="png")
                await browser.close()
                log.info(f"Today screenshot: {len(screenshot_bytes):,} bytes")
                return screenshot_bytes
        except Exception as exc:
            log.error(f"Playwright today screenshot failed: {exc}", exc_info=True)
            return None

    async def _playwright_screenshot_week(self) -> Optional[bytes]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"])
                context = await browser.new_context(locale="en-US", timezone_id="Africa/Addis_Ababa", viewport={"width": 1280, "height": 1800}, user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                page = await context.new_page()
                await page.goto("https://www.forexfactory.com/calendar", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=PLAYWRIGHT_TIMEOUT_MS)
                await page.wait_for_timeout(PLAYWRIGHT_EXTRA_WAIT_MS)
                await page.evaluate("""
                    document.querySelectorAll('.calendar__row').forEach(row => {
                        const imp = row.querySelector('.calendar__impact span');
                        const curr = row.querySelector('.calendar__currency');
                        let hide = false;
                        if (imp) {
                            const cls = imp.className;
                            if (!cls.includes('high') && !cls.includes('medium')) { hide = true; }
                        }
                        if (curr && curr.innerText.trim().toUpperCase() !== 'USD') {
                            hide = true;
                        }
                        if (hide) { row.style.display = 'none'; }
                    });
                """)
                table = await page.query_selector(".calendar__table")
                screenshot_bytes = await table.screenshot(type="png") if table else await page.screenshot(full_page=True, type="png")
                await browser.close()
                log.info(f"Week screenshot: {len(screenshot_bytes):,} bytes")
                return screenshot_bytes
        except Exception as exc:
            log.error(f"Playwright week screenshot failed: {exc}", exc_info=True)
            return None

    async def _extract_events_from_page(self, page) -> List[dict]:
        events = []
        current_date = ""
        try:
            rows = await page.query_selector_all(".calendar__row--event")
            log.debug(f"Found {len(rows)} event rows in calendar")
            for row in rows:
                try:
                    date_cell = await row.query_selector(".calendar__cell.calendar__date")
                    if date_cell:
                        date_text = (await date_cell.inner_text()).strip()
                        if date_text:
                            current_date = date_text
                    impact_el = await row.query_selector(".calendar__impact span")
                    if not impact_el:
                        continue
                    impact_class = await impact_el.get_attribute("class") or ""
                    # Improved detection: check both "high" and "impact--high"
                    if "high" in impact_class or "impact--high" in impact_class:
                        impact = "red"
                    elif "medium" in impact_class or "impact--medium" in impact_class:
                        impact = "orange"
                    else:
                        continue  # skip low impact
                    time_el = await row.query_selector(".calendar__cell.calendar__time")
                    time_raw = (await time_el.inner_text()).strip() if time_el else ""
                    currency_el = await row.query_selector(".calendar__cell.calendar__currency")
                    currency = (await currency_el.inner_text()).strip() if currency_el else "—"
                    event_el = await row.query_selector(".calendar__cell.calendar__event")
                    event_name = (await event_el.inner_text()).strip() if event_el else "Unknown"
                    forecast_el = await row.query_selector(".calendar__cell.calendar__forecast")
                    forecast = ((await forecast_el.inner_text()).strip() if forecast_el else "") or "—"
                    previous_el = await row.query_selector(".calendar__cell.calendar__previous")
                    previous = ((await previous_el.inner_text()).strip() if previous_el else "") or "—"
                    time_12h, time_24h = self._parse_ff_time(time_raw)
                    events.append({
                        "date": current_date,
                        "time_raw": time_raw,
                        "time_12h": time_12h,
                        "time_24h": time_24h,
                        "currency": currency,
                        "name": event_name,
                        "impact": impact,
                        "forecast": forecast,
                        "previous": previous
                    })
                except Exception as exc:
                    log.debug(f"Row parse error: {exc}")
                    continue
        except Exception as exc:
            log.error(f"Event extraction error: {exc}", exc_info=True)
        return events

    @staticmethod
    def _parse_ff_time(time_str: str) -> Tuple[str, str]:
        if not time_str or time_str in ("All Day", "Tentative", ""):
            return ("All Day", "")
        try:
            clean = time_str.replace("\u202f", " ").strip().lower()
            dt = datetime.strptime(clean, "%I:%M%p")
            return (dt.strftime("%I:%M %p"), dt.strftime("%H:%M"))
        except ValueError:
            try:
                dt = datetime.strptime(time_str.strip().lower(), "%I%p")
                return (dt.strftime("%I:%M %p"), dt.strftime("%H:%M"))
            except ValueError:
                return (time_str, "")

    async def _process_channel(self, channel: str):
        if not await self._ensure_connected():
            log.warning(f"Skipping {channel} — not connected.")
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
        log.info(f"📨  {len(collected)} new message(s) from {channel}")
        for msg in collected:
            await self._handle_message(msg, channel)
            await asyncio.sleep(random.uniform(2, 6))
        await self._mem.set_last_msg_id(channel, new_last_id)

    async def _handle_message(self, msg, source_channel: str):
        text = msg.text or msg.message or ""
        image_data: Optional[bytes] = None
        image_mime = "image/jpeg"
        if msg.media and _is_image(msg):
            try:
                buf = io.BytesIO()
                await self._client.download_media(msg.media, file=buf)
                image_data = buf.getvalue()
                image_mime = _doc_mime(msg)
                log.debug(f"Image: {len(image_data):,} bytes | mime={image_mime}")
            except Exception as exc:
                log.warning(f"Image download failed: {exc}")
        
        is_duplicate = False
        text_hash = None
        img_hash = None
        
        if text:
            text_hash = self._mem.hash_combined(text, None)
            if await self._mem.is_duplicate(text_hash):
                is_duplicate = True
                
        if image_data:
            img_hash = self._mem.hash_combined("", image_data)
            if await self._mem.is_duplicate(img_hash):
                is_duplicate = True

        if is_duplicate:
            log.info(f"[SKIP] Duplicate detected — Text or Image already seen.")
            return

        if text_hash:
            await self._mem.mark_seen(text_hash, source=source_channel)
        if img_hash:
            await self._mem.mark_seen(img_hash, source=source_channel)

        log.info(f"🔍  Analysing msg {msg.id} from {source_channel} | text={len(text)}c | image={'✅' if image_data else '❌'}")
        verdict = await self._ai.analyse(text, image_data, image_mime)
        
        content_hash = self._mem.hash_combined(text, image_data)
        
        if not verdict.get("approved"):
            log.info(f"[REJECTED] engine={verdict.get('engine')} | reason='{verdict.get('reason')}' | issues={verdict.get('issues')}")
            return
        
        post_text = self._build_post(verdict)
        
        delay = random.uniform(self._min_delay, self._max_delay)
        log.info(f"⏳  Waiting {delay:.1f}s before posting …")
        await asyncio.sleep(delay)
        await self._simulate_typing(len(post_text))
        
        sent = await self._broadcast_media(post_text, image_data, image_mime)
        if sent is None:
            return
            
        await self._mem.log_posted(source_channel=source_channel, source_msg_id=msg.id, dest_msg_id=sent.id, content_hash=content_hash, ai_verdict=verdict, formatted_text=post_text)
        log.info(f"✅  Posted → msg_id={sent.id} | engine={verdict.get('engine')} | confidence={verdict.get('confidence')}")

    @staticmethod
    def _build_post(verdict: dict) -> str:
        body = verdict.get("formatted_text", "").strip()
        tags = verdict.get("hashtags", "").strip()
        if tags and not body.endswith(tags):
            body = f"{body}\n\n{tags}"
            
        if random.random() < 0.30:
            body += "\n\n💡 [Squad 4xx](https://t.me/Squad_4xx)"
            
        return body

    async def _simulate_typing(self, text_len: int):
        duration = min(max(text_len / 180, 2), 14)
        if self._dest_channels:
            try:
                async with self._client.action(self._dest_channels[0], "typing"):
                    await asyncio.sleep(duration)
            except Exception as exc:
                log.debug(f"Typing action skipped: {exc}")

    async def _send_text(self, text: str):
        return await self._broadcast_text(text)

    async def _send_file_with_caption(self, file_bytes: bytes, mime: str, caption: str):
        return await self._broadcast_file_with_caption(file_bytes, mime, caption)

    async def _send(self, text: str, image_data: Optional[bytes], image_mime: str):
        return await self._broadcast_media(text, image_data, image_mime)

    async def reminder_dispatcher_loop(self):
        log.info("🔔  Reminder dispatcher loop running …")
        while True:
            try:
                await self._check_reminders()
            except Exception as exc:
                log.error(f"Reminder dispatcher error: {exc}", exc_info=True)
            await asyncio.sleep(60)
