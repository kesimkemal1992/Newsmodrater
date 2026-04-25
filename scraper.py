"""
scraper.py — Telethon channel scraper, forwarder, and Forex Factory scheduler.

Features:
  • Scrapes Telegram channels and forwards approved news (existing flow)
  • Scrapes ForexFactory.com via Playwright (Red + Orange events only)
  • Posts Daily Briefing at 07:00 AM EAT
  • Posts 10-minute alerts as REPLIES to the morning briefing (max 2/day)
    — Automatically prioritises FOMC, NFP, Interest Rate, CPI events
  • Posts Weekly Outlook every Sunday at 09:00 PM EAT (screenshot + text)
  • Auto-reconnect on Telethon ConnectionError

Timezone: Africa/Addis_Ababa (GMT+3) — EAT.
"""

import asyncio
import io
import logging
import mimetypes
import random
import json
from datetime import datetime, timedelta, timezone
from typing import Optional, List

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

# ── High-priority event keywords (always get reminder slots first) ────────────
_PRIORITY_KEYWORDS = [
    "fomc", "federal open market committee", "interest rate decision",
    "nfp", "non-farm payroll", "non-farm", "cpi", "consumer price index",
    "pce", "gdp", "fed chair", "jerome powell", "rate decision",
    "bank of england", "ecb", "european central bank", "boe",
    "boj", "bank of japan", "rba", "bank of canada",
]


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


def _eat_date_display() -> str:
    return _eat_now().strftime("%A, %B %d, %Y")


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

        # Daily scheduler state
        self._daily_briefing_posted_date: Optional[str] = None
        self._weekly_posted_date: Optional[str] = None  # "YYYY-WW"
        self._todays_events: List[dict] = []

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

    async def stop(self):
        await self._client.disconnect()

    async def _ensure_connected(self):
        """Reconnect Telethon client if disconnected."""
        if not self._client.is_connected():
            log.warning("Telethon disconnected — reconnecting …")
            try:
                await self._client.connect()
                if not await self._client.is_user_authorized():
                    log.error("Session expired after reconnect — cannot reauthorize automatically.")
                    return False
                log.info("✅  Reconnected successfully.")
            except Exception as exc:
                log.error(f"Reconnect failed: {exc}")
                return False
        return True

    # ── Main poll cycle ────────────────────────────────────────────────────────
    async def poll_and_forward(self):
        stats = await self._mem.stats()
        log.info(
            f"Poll cycle | sources={len(self._sources)} | "
            f"calendar={'❌' if not self._cfg.get('calendar_source') else '✅'} | "
            f"hashes={stats['tracked_hashes']} | "
            f"posted_24h={stats['posted_last_24h']} | "
            f"pending_reminders={stats.get('pending_reminders', 0)}"
        )

        # ── Ensure connected before doing anything ─────────────────────────────
        if not await self._ensure_connected():
            log.warning("Skipping poll cycle — not connected.")
            return

        # ── Scheduler checks (run every cycle) ────────────────────────────────
        await self._check_daily_briefing()
        await self._check_reminders()
        await self._check_weekly_outlook()

        # ── Telegram channel scraping ──────────────────────────────────────────
        for channel in self._sources:
            try:
                await self._process_channel(channel)
            except FloodWaitError as fwe:
                log.warning(f"FloodWait {fwe.seconds}s — sleeping …")
                await asyncio.sleep(fwe.seconds + 5)
            except Exception as exc:
                log.error(f"Error on channel {channel}: {exc}", exc_info=True)
                await asyncio.sleep(5)

    # ── Daily Briefing Scheduler ───────────────────────────────────────────────
    async def _check_daily_briefing(self):
        """Post daily briefing at 07:00 AM EAT if not already posted today."""
        now = _eat_now()
        today_str = _eat_today_str()

        # Only post between 07:00 and 09:00 to avoid late posts
        if not (7 <= now.hour < 9):
            return

        if await self._mem.has_daily_briefing(today_str):
            return  # Already posted today

        log.info(f"📅  Daily briefing time! Scraping ForexFactory for {today_str} …")
        events = await self._scrape_forex_factory_today()

        if not events:
            log.info("No high-impact events found today — skipping daily briefing.")
            # Still mark as "posted" with id=-1 so we don't retry all day
            await self._mem.save_daily_briefing(today_str, -1, [])
            return

        self._todays_events = events
        date_display = f"*{now.strftime('%A, %B %d, %Y')}*"
        briefing_text = await self._ai.generate_daily_briefing(events, date_display)

        if not briefing_text:
            log.error("Failed to generate daily briefing text.")
            return

        # Take today's ForexFactory screenshot and send as photo + caption
        log.info("📸  Taking today's ForexFactory screenshot …")
        screenshot = await self._take_forex_factory_screenshot_today()
        if screenshot:
            sent = await self._send_file_with_caption(screenshot, "image/png", briefing_text)
            log.info(f"📅  Daily briefing sent as photo+caption → msg_id={sent.id if sent else 'FAILED'}")
        else:
            log.warning("Daily screenshot failed — sending text only.")
            sent = await self._send_text(briefing_text)

        if sent:
            await self._mem.save_daily_briefing(today_str, sent.id, events)
            log.info(f"📅  Daily briefing posted → msg_id={sent.id}")
        else:
            log.error("Failed to send daily briefing.")

    # ── Reminder Scheduler ─────────────────────────────────────────────────────
    async def _check_reminders(self):
        """Check if any event is 10 minutes away and post a reminder (max 2/day)."""
        today_str = _eat_today_str()

        # Check daily cap
        reminder_count = await self._mem.get_reminder_count_today(today_str)
        if reminder_count >= 2:
            return  # Daily cap reached

        # Get the briefing msg_id to reply to
        briefing_msg_id = await self._mem.get_daily_briefing_msg_id(today_str)
        if not briefing_msg_id or briefing_msg_id == -1:
            return  # No briefing posted today, or no events

        events = self._todays_events
        if not events:
            # Try to recover events from DB
            async with self._mem._db.execute(
                "SELECT events_json FROM daily_briefings WHERE date_str=?", (today_str,)
            ) as cur:
                row = await cur.fetchone()
            if row and row["events_json"]:
                events = json.loads(row["events_json"])
            if not events:
                return

        now = _eat_now()
        now_naive = now.replace(tzinfo=None)

        # Select up to 2 most critical events for reminders
        # Priority: high-priority keywords first, then by time
        pending_events = []
        for event in events:
            event_key = f"{today_str}_{event.get('name', '')}_{event.get('currency', '')}"
            if await self._mem.has_reminder_been_sent(event_key):
                continue

            # Parse event time (stored as "HH:MM" in EAT, 24-hour)
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
            if 8 <= minutes_until <= 12:  # Within the 10-minute window (8-12 min buffer)
                pending_events.append((event, event_key, _is_priority_event(event.get("name", ""))))

        if not pending_events:
            return

        # Sort: priority events first
        pending_events.sort(key=lambda x: (not x[2], x[0].get("time_24h", "")))

        # Send only as many as remaining daily cap allows
        slots_left = 2 - reminder_count
        for event, event_key, is_priority in pending_events[:slots_left]:
            await self._send_reminder(event, event_key, briefing_msg_id, today_str)
            await asyncio.sleep(2)

    async def _send_reminder(self, event: dict, event_key: str, reply_to_msg_id: int, today_str: str):
        """Generate and send a 10-minute reminder as a reply to the morning briefing."""
        log.info(f"⏰  Sending 10-min reminder for: {event.get('name')}")
        alert_text = await self._ai.generate_alert(event)

        if not alert_text:
            log.error(f"Failed to generate alert for {event.get('name')}")
            return

        try:
            sent = await self._client.send_message(
                self._dest,
                alert_text,
                parse_mode="md",
                reply_to=reply_to_msg_id,
            )
            if sent:
                await self._mem.mark_reminder_sent(event_key)
                await self._mem.increment_reminder_count(today_str)
                log.info(f"🚨  Reminder sent → msg_id={sent.id} (reply to {reply_to_msg_id})")
        except Exception as exc:
            log.error(f"Failed to send reminder: {exc}", exc_info=True)

    # ── Weekly Outlook Scheduler ───────────────────────────────────────────────
    async def _check_weekly_outlook(self):
        """Post weekly outlook every Sunday at 21:00 EAT."""
        now = _eat_now()
        if now.weekday() != 6:  # 6 = Sunday
            return
        if now.hour != 21:
            return

        week_key = now.strftime("%Y-%W")
        if self._weekly_posted_date == week_key:
            return  # Already posted this week

        log.info("📆  Sunday weekly outlook time! Scraping ForexFactory for the week …")
        events = await self._scrape_forex_factory_week()

        if not events:
            log.info("No high-impact events this week — skipping weekly outlook.")
            self._weekly_posted_date = week_key
            return

        week_start = now + timedelta(days=1)  # Monday
        week_end = week_start + timedelta(days=4)  # Friday
        week_range = (
            f"{week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}"
        )

        outlook_text = await self._ai.generate_weekly_outlook(events, week_range)
        if not outlook_text:
            log.error("Failed to generate weekly outlook.")
            return

        # Take weekly ForexFactory screenshot and send as photo + caption
        log.info("📸  Taking weekly ForexFactory screenshot …")
        screenshot = await self._take_forex_factory_screenshot_week()
        if screenshot:
            sent = await self._send_file_with_caption(screenshot, "image/png", outlook_text)
            log.info(f"📆  Weekly outlook sent as photo+caption → msg_id={sent.id if sent else 'FAILED'}")
        else:
            log.warning("Weekly screenshot failed — sending text only.")
            sent = await self._send_text(outlook_text)

        if sent:
            self._weekly_posted_date = week_key
            log.info(f"📆  Weekly outlook posted → msg_id={sent.id}")

    # ── ForexFactory Scrapers ──────────────────────────────────────────────────
    async def _scrape_forex_factory_today(self) -> List[dict]:
        """Scrape today's Red + Orange impact events from ForexFactory."""
        return await asyncio.get_event_loop().run_in_executor(
            None, self._playwright_scrape_today
        )

    async def _scrape_forex_factory_week(self) -> List[dict]:
        """Scrape this week's Red + Orange impact events from ForexFactory."""
        return await asyncio.get_event_loop().run_in_executor(
            None, self._playwright_scrape_week
        )

    async def _take_forex_factory_screenshot_today(self) -> Optional[bytes]:
        """Take a screenshot of today's ForexFactory calendar (daily view)."""
        return await asyncio.get_event_loop().run_in_executor(
            None, self._playwright_screenshot_today
        )

    async def _take_forex_factory_screenshot_week(self) -> Optional[bytes]:
        """Take a screenshot of this week's ForexFactory calendar (weekly view)."""
        return await asyncio.get_event_loop().run_in_executor(
            None, self._playwright_screenshot_week
        )

    def _playwright_scrape_today(self) -> List[dict]:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox",
                          "--disable-dev-shm-usage", "--disable-gpu"],
                )
                context = browser.new_context(
                    locale="en-US",
                    timezone_id="Africa/Addis_Ababa",
                    user_agent=(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                    ),
                )
                page = context.new_page()
                page.goto("https://www.forexfactory.com/calendar", timeout=30_000)
                page.wait_for_selector(".calendar__table", timeout=15_000)
                events = self._extract_events_from_page(page, single_day=True)
                browser.close()
                return events
        except Exception as exc:
            log.error(f"Playwright today scrape failed: {exc}", exc_info=True)
            return []

    def _playwright_scrape_week(self) -> List[dict]:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox",
                          "--disable-dev-shm-usage", "--disable-gpu"],
                )
                context = browser.new_context(
                    locale="en-US",
                    timezone_id="Africa/Addis_Ababa",
                    user_agent=(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                    ),
                )
                page = context.new_page()
                page.goto("https://www.forexfactory.com/calendar", timeout=30_000)
                page.wait_for_selector(".calendar__table", timeout=15_000)
                events = self._extract_events_from_page(page, single_day=False)
                browser.close()
                return events
        except Exception as exc:
            log.error(f"Playwright week scrape failed: {exc}", exc_info=True)
            return []

    def _playwright_screenshot_today(self) -> Optional[bytes]:
        """Screenshot ForexFactory filtered to TODAY only — used for daily briefing."""
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox",
                          "--disable-dev-shm-usage", "--disable-gpu"],
                )
                context = browser.new_context(
                    locale="en-US",
                    timezone_id="Africa/Addis_Ababa",
                    viewport={"width": 1280, "height": 1000},
                    user_agent=(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                    ),
                )
                page = context.new_page()
                # Use ?day=today to force ForexFactory to today's view
                page.goto("https://www.forexfactory.com/calendar?day=today", timeout=30_000)
                page.wait_for_selector(".calendar__table", timeout=15_000)
                # Hide low-impact rows, keep only red + orange
                page.evaluate("""
                    document.querySelectorAll('.calendar__row').forEach(row => {
                        const imp = row.querySelector('.calendar__impact span');
                        if (imp) {
                            const cls = imp.className;
                            if (!cls.includes('high') && !cls.includes('medium')) {
                                row.style.display = 'none';
                            }
                        }
                    });
                    // Hide weekend/other-day header rows that are empty
                    document.querySelectorAll('.calendar__row--day-breaker').forEach(row => {
                        row.style.display = 'none';
                    });
                """)
                # Crop tightly to the calendar table only
                table = page.query_selector(".calendar__table")
                if table:
                    screenshot_bytes = table.screenshot(type="png")
                else:
                    screenshot_bytes = page.screenshot(
                        clip={"x": 0, "y": 0, "width": 1280, "height": 1000},
                        type="png",
                    )
                browser.close()
                log.info(f"Today screenshot taken: {len(screenshot_bytes):,} bytes")
                return screenshot_bytes
        except Exception as exc:
            log.error(f"Playwright today screenshot failed: {exc}", exc_info=True)
            return None

    def _playwright_screenshot_week(self) -> Optional[bytes]:
        """Screenshot ForexFactory full week view — used for Sunday weekly outlook."""
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox",
                          "--disable-dev-shm-usage", "--disable-gpu"],
                )
                context = browser.new_context(
                    locale="en-US",
                    timezone_id="Africa/Addis_Ababa",
                    viewport={"width": 1280, "height": 1800},
                    user_agent=(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                    ),
                )
                page = context.new_page()
                # Default calendar view = current week
                page.goto("https://www.forexfactory.com/calendar", timeout=30_000)
                page.wait_for_selector(".calendar__table", timeout=15_000)
                # Hide low-impact rows, keep red + orange only
                page.evaluate("""
                    document.querySelectorAll('.calendar__row').forEach(row => {
                        const imp = row.querySelector('.calendar__impact span');
                        if (imp) {
                            const cls = imp.className;
                            if (!cls.includes('high') && !cls.includes('medium')) {
                                row.style.display = 'none';
                            }
                        }
                    });
                """)
                # Full-page screenshot of the calendar table
                table = page.query_selector(".calendar__table")
                if table:
                    screenshot_bytes = table.screenshot(type="png")
                else:
                    screenshot_bytes = page.screenshot(
                        full_page=True,
                        type="png",
                    )
                browser.close()
                log.info(f"Week screenshot taken: {len(screenshot_bytes):,} bytes")
                return screenshot_bytes
        except Exception as exc:
            log.error(f"Playwright week screenshot failed: {exc}", exc_info=True)
            return None

    def _extract_events_from_page(self, page, single_day: bool = True) -> List[dict]:
        """Extract Red + Orange events from the ForexFactory calendar page."""
        events = []
        current_date = ""

        try:
            rows = page.query_selector_all(".calendar__row--event")
            for row in rows:
                try:
                    # Date cell (only appears on first event of each day)
                    date_cell = row.query_selector(".calendar__cell.calendar__date")
                    if date_cell:
                        date_text = date_cell.inner_text().strip()
                        if date_text:
                            current_date = date_text

                    # Impact level
                    impact_el = row.query_selector(".calendar__impact span")
                    if not impact_el:
                        continue
                    impact_class = impact_el.get_attribute("class") or ""
                    if "high" in impact_class:
                        impact = "red"
                    elif "medium" in impact_class:
                        impact = "orange"
                    else:
                        continue  # Skip low-impact events

                    # Time
                    time_el = row.query_selector(".calendar__cell.calendar__time")
                    time_raw = time_el.inner_text().strip() if time_el else ""

                    # Currency
                    currency_el = row.query_selector(".calendar__cell.calendar__currency")
                    currency = currency_el.inner_text().strip() if currency_el else "—"

                    # Event name
                    event_el = row.query_selector(".calendar__cell.calendar__event")
                    event_name = event_el.inner_text().strip() if event_el else "Unknown"

                    # Forecast + Previous
                    forecast_el = row.query_selector(".calendar__cell.calendar__forecast")
                    forecast = forecast_el.inner_text().strip() if forecast_el else "—"
                    if not forecast:
                        forecast = "—"

                    previous_el = row.query_selector(".calendar__cell.calendar__previous")
                    previous = previous_el.inner_text().strip() if previous_el else "—"
                    if not previous:
                        previous = "—"

                    # Convert time to 12-hour and 24-hour formats
                    time_12h, time_24h = self._parse_ff_time(time_raw)

                    events.append({
                        "date": current_date,
                        "time_raw": time_raw,
                        "time_12h": time_12h,
                        "time_24h": time_24h,
                        "currency": currency,
                        "name": event_name,
                        "impact": impact,
                        "forecast": forecast if forecast else "—",
                        "previous": previous if previous else "—",
                    })
                except Exception as exc:
                    log.debug(f"Row parse error: {exc}")
                    continue
        except Exception as exc:
            log.error(f"Event extraction error: {exc}", exc_info=True)

        log.info(f"ForexFactory: extracted {len(events)} high/medium-impact events.")
        return events

    @staticmethod
    def _parse_ff_time(time_str: str):
        """Convert ForexFactory time (e.g. '8:30am') to 12h and 24h formats."""
        if not time_str or time_str in ("All Day", "Tentative", ""):
            return ("All Day", "")
        try:
            # ForexFactory uses formats like "8:30am", "10:00am", "2:00pm"
            time_str_clean = time_str.replace("\u202f", " ").strip().lower()
            dt = datetime.strptime(time_str_clean, "%I:%M%p")
            return (dt.strftime("%I:%M %p"), dt.strftime("%H:%M"))
        except ValueError:
            try:
                dt = datetime.strptime(time_str.strip().lower(), "%I%p")
                return (dt.strftime("%I:%M %p"), dt.strftime("%H:%M"))
            except ValueError:
                return (time_str, "")

    # ── Channel Scraping (existing flow) ──────────────────────────────────────
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
        except Exception as exc:
            log.error(f"iter_messages error on {channel}: {exc}", exc_info=True)
            # Attempt reconnect for next cycle
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

    # ── Per-message handler ────────────────────────────────────────────────────
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

        content_hash = self._mem.hash_combined(text, image_data)
        if await self._mem.is_duplicate(content_hash):
            log.info(f"[SKIP] Duplicate — hash={content_hash[:12]}…")
            return

        log.info(
            f"🔍  Analysing msg {msg.id} from {source_channel} | "
            f"text={len(text)}c | image={'✅' if image_data else '❌'}"
        )
        verdict = await self._ai.analyse(text, image_data, image_mime)
        await self._mem.mark_seen(content_hash, source=source_channel)

        if not verdict.get("approved"):
            log.info(
                f"[REJECTED] engine={verdict.get('engine')} | "
                f"reason='{verdict.get('reason')}' | "
                f"issues={verdict.get('issues')}"
            )
            return

        post_text = self._build_post(verdict)
        delay = random.uniform(self._min_delay, self._max_delay)
        log.info(f"⏳  Waiting {delay:.1f}s before posting …")
        await asyncio.sleep(delay)

        await self._simulate_typing(len(post_text))

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

    async def _send_text(self, text: str):
        """Send a plain text message to the destination channel."""
        try:
            return await self._client.send_message(
                self._dest, text, parse_mode="md"
            )
        except ChatWriteForbiddenError:
            log.error("❌  Cannot post — check admin rights.")
        except FloodWaitError as fwe:
            log.warning(f"FloodWait {fwe.seconds}s — retrying …")
            await asyncio.sleep(fwe.seconds + 3)
            return await self._send_text(text)
        except Exception as exc:
            log.error(f"Send error: {exc}", exc_info=True)
        return None

    async def _send_file_with_caption(
        self, file_bytes: bytes, mime: str, caption: str
    ):
        """Send a screenshot file with a caption to the destination channel.

        Telethon requires the BytesIO object to have a .name attribute with
        a valid extension so it can detect the file type correctly.
        We set it explicitly and seek to 0 before sending.
        """
        try:
            ext = mimetypes.guess_extension(mime) or ".png"
            buf = io.BytesIO(file_bytes)
            buf.name = f"weekly_calendar{ext}"
            buf.seek(0)
            return await self._client.send_file(
                self._dest,
                buf,
                caption=caption,
                parse_mode="md",
                force_document=False,
            )
        except Exception as exc:
            log.error(f"Send screenshot error: {exc}", exc_info=True)
            log.info("Falling back to text-only weekly outlook.")
            return await self._send_text(caption)

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
            log.error("❌  Cannot post — check admin rights.")
        except FloodWaitError as fwe:
            log.warning(f"FloodWait {fwe.seconds}s — retrying …")
            await asyncio.sleep(fwe.seconds + 3)
            return await self._send(text, image_data, image_mime)
        except Exception as exc:
            log.error(f"Send error: {exc}", exc_info=True)
        return None

    # ── Reminder Dispatcher (external interface for main.py) ──────────────────
    async def reminder_dispatcher_loop(self):
        """
        Dedicated reminder loop — checks every 60 seconds for upcoming events.
        Runs concurrently with the main poll loop.
        """
        log.info("🔔  Reminder dispatcher loop running …")
        while True:
            try:
                await self._check_reminders()
            except Exception as exc:
                log.error(f"Reminder dispatcher error: {exc}", exc_info=True)
            await asyncio.sleep(60)
