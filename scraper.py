"""
scraper.py — Telethon channel scraper, forwarder, and Forex Factory scheduler.

Features:
  • Scrapes Telegram channels and forwards approved news (moderation flow)
  • Scrapes ForexFactory.com via Playwright (Red + Orange events only)
  • Posts Daily Briefing at 07:00 AM EAT as PHOTO + caption (today screenshot)
  • Posts 10-min alerts as REPLIES to the morning briefing (max 2/day)
    — Look-Ahead Priority Strategy: reserves slots for Top 2 VIP events of the day
    — FOMC, NFP, CPI, Rate Decisions, Fed Chair always take the 2 slots
    — Lower-priority events are SKIPPED if a VIP event comes later that day
  • Posts Weekly Outlook every Sunday at 09:00 PM EAT as PHOTO + caption
  • Auto-reconnect on Telethon ConnectionError

Timezone: Africa/Addis_Ababa (GMT+3) — EAT.
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

# ── Priority keywords — these events ALWAYS claim the 2 reminder slots ─────────
# If today has any of these, lower-priority events are SKIPPED entirely
_PRIORITY_KEYWORDS = [
    "fomc",
    "federal open market committee",
    "interest rate decision",
    "rate decision",
    "nfp",
    "non-farm payroll",
    "non-farm payrolls",
    "cpi",
    "consumer price index",
    "pce",
    "core pce",
    "gdp",
    "fed chair",
    "powell speaks",
    "jerome powell",
    "bank of england",
    "boe rate",
    "ecb rate",
    "european central bank",
    "boj rate",
    "bank of japan",
    "rba rate",
    "bank of canada",
    "unemployment rate",
    "retail sales",
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

        # In-memory state for today's events and VIP reservation list
        self._todays_events: List[dict] = []
        self._todays_vip_events: List[dict] = []   # Top 2 reserved for reminders
        self._daily_briefing_posted_date: Optional[str] = None
        self._weekly_posted_date: Optional[str] = None  # "YYYY-WW"

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

    async def _ensure_connected(self) -> bool:
        """Reconnect Telethon client if disconnected. Returns True if connected."""
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
            f"hashes={stats['tracked_hashes']} | "
            f"posted_24h={stats['posted_last_24h']} | "
            f"reminders_today={stats.get('pending_reminders', 0)}"
        )

        if not await self._ensure_connected():
            log.warning("Skipping poll cycle — not connected.")
            return

        # Scheduler checks run every poll cycle
        await self._check_daily_briefing()
        await self._check_reminders()
        await self._check_weekly_outlook()

        # Telegram channel scraping
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
        """Post daily briefing at 07:00 AM EAT as PHOTO + caption."""
        now = _eat_now()
        today_str = _eat_today_str()

        # Post window: 07:00–09:00 EAT only
        if not (7 <= now.hour < 9):
            return

        if await self._mem.has_daily_briefing(today_str):
            return  # Already posted today

        log.info(f"📅  Daily briefing time! Scraping ForexFactory for {today_str} …")
        events = await self._scrape_forex_factory_today()

        if not events:
            log.info("No high-impact events today — skipping daily briefing.")
            await self._mem.save_daily_briefing(today_str, -1, [])
            return

        # Store events and compute VIP reservation list for reminders
        self._todays_events = events
        self._todays_vip_events = self._select_vip_events(events)
        log.info(
            f"VIP reminder slots reserved for: "
            f"{[e.get('name') for e in self._todays_vip_events]}"
        )

        date_display = now.strftime("%A, %B %d, %Y")
        briefing_text = await self._ai.generate_daily_briefing(events, date_display)

        if not briefing_text:
            log.error("Failed to generate daily briefing text.")
            return

        # Take today's screenshot and send as PHOTO + caption
        log.info("📸  Taking today's ForexFactory screenshot …")
        screenshot = await self._take_forex_factory_screenshot_today()

        if screenshot:
            sent = await self._send_file_with_caption(screenshot, "image/png", briefing_text)
        else:
            log.warning("Today screenshot failed — sending text only.")
            sent = await self._send_text(briefing_text)

        if sent:
            await self._mem.save_daily_briefing(today_str, sent.id, events)
            log.info(f"📅  Daily briefing posted → msg_id={sent.id}")
        else:
            log.error("Failed to send daily briefing.")

    # ── Look-Ahead VIP Event Selection ────────────────────────────────────────
    def _select_vip_events(self, events: List[dict]) -> List[dict]:
        """
        Look-Ahead Priority Strategy:
        Evaluate ALL of today's Red events. Sort by priority tier then time.
        Reserve ONLY the Top 2 events for reminder slots.
        Lower-priority events are excluded — they will never get a reminder slot
        if a VIP event is scheduled later that day.

        Tier 1 (highest): _PRIORITY_KEYWORDS match (FOMC, NFP, CPI, Rate decisions etc.)
        Tier 2: All other Red events
        Orange events: Never get reminder slots — Red only.
        """
        # Filter: Red impact only for reminders
        red_events = [e for e in events if e.get("impact") == "red"]

        if not red_events:
            # No red events → fall back to orange if needed
            red_events = events

        # Sort by: Tier 1 first, then by time
        def sort_key(event):
            is_priority = _is_priority_event(event.get("name", ""))
            time_str = event.get("time_24h", "99:99")
            return (0 if is_priority else 1, time_str)

        sorted_events = sorted(red_events, key=sort_key)

        # Reserve Top 2 slots
        vip = sorted_events[:2]
        log.info(
            f"Look-Ahead: {len(red_events)} red events today. "
            f"Top 2 VIP slots: {[e.get('name') for e in vip]}"
        )
        return vip

    # ── Reminder Scheduler ─────────────────────────────────────────────────────
    async def _check_reminders(self):
        """
        Check if any VIP event is 10 minutes away and post a reminder.
        Uses the Look-Ahead reservation list — ONLY sends alerts for VIP events.
        Max 2 per day enforced via memory DB.
        """
        today_str = _eat_today_str()

        # Check daily cap
        reminder_count = await self._mem.get_reminder_count_today(today_str)
        if reminder_count >= 2:
            return

        # Get the briefing msg_id to reply to
        briefing_msg_id = await self._mem.get_daily_briefing_msg_id(today_str)
        if not briefing_msg_id or briefing_msg_id == -1:
            return

        # Recover VIP events if in-memory list was lost (e.g. after restart)
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

            event_key = (
                f"{today_str}_{event.get('name', '')}_{event.get('currency', '')}"
            )

            # Skip if reminder already sent for this event
            if await self._mem.has_reminder_been_sent(event_key):
                continue

            # Parse event time (24h stored in EAT)
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

            # Trigger window: 8–12 minutes before (centred on 10 min)
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
        """Generate and send a 10-minute reminder as a reply to the morning briefing."""
        log.info(f"⏰  Sending 10-min reminder for: {event.get('name')}")
        # Get rotating motivational line index from memory (cycles across all reminders)
        mot_index = await self._mem.get_and_increment_motivational_index()
        alert_text = await self._ai.generate_alert(event, motivational_index=mot_index)

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
                log.info(
                    f"🚨  Reminder sent → msg_id={sent.id} "
                    f"(reply to briefing msg {reply_to_msg_id})"
                )
        except Exception as exc:
            log.error(f"Failed to send reminder: {exc}", exc_info=True)

    # ── Weekly Outlook Scheduler ───────────────────────────────────────────────
    async def _check_weekly_outlook(self):
        """Post weekly outlook every Sunday at 21:00 EAT as PHOTO + caption."""
        now = _eat_now()
        if now.weekday() != 6:  # 6 = Sunday
            return
        if now.hour != 21:
            return

        week_key = now.strftime("%Y-%W")
        if self._weekly_posted_date == week_key:
            return

        log.info("📆  Sunday weekly outlook time! Scraping ForexFactory for the week …")
        events = await self._scrape_forex_factory_week()

        if not events:
            log.info("No high-impact events this week — skipping weekly outlook.")
            self._weekly_posted_date = week_key
            return

        week_start = now + timedelta(days=1)   # Monday
        week_end = week_start + timedelta(days=4)  # Friday
        week_range = (
            f"{week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}"
        )

        outlook_text = await self._ai.generate_weekly_outlook(events, week_range)
        if not outlook_text:
            log.error("Failed to generate weekly outlook.")
            return

        # Take weekly screenshot and send as PHOTO + caption
        log.info("📸  Taking weekly ForexFactory screenshot …")
        screenshot = await self._take_forex_factory_screenshot_week()

        if screenshot:
            sent = await self._send_file_with_caption(screenshot, "image/png", outlook_text)
        else:
            log.warning("Weekly screenshot failed — sending text only.")
            sent = await self._send_text(outlook_text)

        if sent:
            self._weekly_posted_date = week_key
            log.info(f"📆  Weekly outlook posted → msg_id={sent.id}")

    # ── ForexFactory Async Wrappers ────────────────────────────────────────────
    async def _scrape_forex_factory_today(self) -> List[dict]:
        return await asyncio.get_event_loop().run_in_executor(
            None, self._playwright_scrape_today
        )

    async def _scrape_forex_factory_week(self) -> List[dict]:
        return await asyncio.get_event_loop().run_in_executor(
            None, self._playwright_scrape_week
        )

    async def _take_forex_factory_screenshot_today(self) -> Optional[bytes]:
        return await asyncio.get_event_loop().run_in_executor(
            None, self._playwright_screenshot_today
        )

    async def _take_forex_factory_screenshot_week(self) -> Optional[bytes]:
        return await asyncio.get_event_loop().run_in_executor(
            None, self._playwright_screenshot_week
        )

    # ── Playwright: Today Scrape ───────────────────────────────────────────────
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
                page.goto(
                    "https://www.forexfactory.com/calendar?day=today",
                    timeout=30_000,
                )
                page.wait_for_selector(".calendar__table", timeout=15_000)
                events = self._extract_events_from_page(page)
                browser.close()
                log.info(f"Today scrape: {len(events)} high-impact events found.")
                return events
        except Exception as exc:
            log.error(f"Playwright today scrape failed: {exc}", exc_info=True)
            return []

    # ── Playwright: Week Scrape ────────────────────────────────────────────────
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
                page.goto(
                    "https://www.forexfactory.com/calendar",
                    timeout=30_000,
                )
                page.wait_for_selector(".calendar__table", timeout=15_000)
                events = self._extract_events_from_page(page)
                browser.close()
                log.info(f"Week scrape: {len(events)} high-impact events found.")
                return events
        except Exception as exc:
            log.error(f"Playwright week scrape failed: {exc}", exc_info=True)
            return []

    # ── Playwright: Today Screenshot ───────────────────────────────────────────
    def _playwright_screenshot_today(self) -> Optional[bytes]:
        """Screenshot ForexFactory today view — cropped to calendar table only."""
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
                page.goto(
                    "https://www.forexfactory.com/calendar?day=today",
                    timeout=30_000,
                )
                page.wait_for_selector(".calendar__table", timeout=15_000)

                # Hide low-impact rows — keep Red + Orange only
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
                    document.querySelectorAll('.calendar__row--day-breaker').forEach(r => {
                        r.style.display = 'none';
                    });
                """)

                # Crop tightly to calendar table element
                table = page.query_selector(".calendar__table")
                if table:
                    screenshot_bytes = table.screenshot(type="png")
                else:
                    screenshot_bytes = page.screenshot(
                        clip={"x": 0, "y": 0, "width": 1280, "height": 1000},
                        type="png",
                    )
                browser.close()
                log.info(f"Today screenshot: {len(screenshot_bytes):,} bytes")
                return screenshot_bytes
        except Exception as exc:
            log.error(f"Playwright today screenshot failed: {exc}", exc_info=True)
            return None

    # ── Playwright: Week Screenshot ────────────────────────────────────────────
    def _playwright_screenshot_week(self) -> Optional[bytes]:
        """Screenshot ForexFactory full week — used for Sunday weekly outlook."""
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
                page.goto(
                    "https://www.forexfactory.com/calendar",
                    timeout=30_000,
                )
                page.wait_for_selector(".calendar__table", timeout=15_000)

                # Hide low-impact rows — keep Red + Orange only
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

                # Full-height screenshot of the calendar table
                table = page.query_selector(".calendar__table")
                if table:
                    screenshot_bytes = table.screenshot(type="png")
                else:
                    screenshot_bytes = page.screenshot(full_page=True, type="png")

                browser.close()
                log.info(f"Week screenshot: {len(screenshot_bytes):,} bytes")
                return screenshot_bytes
        except Exception as exc:
            log.error(f"Playwright week screenshot failed: {exc}", exc_info=True)
            return None

    # ── Event Extraction ───────────────────────────────────────────────────────
    def _extract_events_from_page(self, page) -> List[dict]:
        """Extract Red + Orange impact events from a ForexFactory calendar page."""
        events = []
        current_date = ""

        try:
            rows = page.query_selector_all(".calendar__row--event")
            for row in rows:
                try:
                    # Date cell (only on first event of each day)
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
                        continue  # Skip low-impact

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
                    forecast = (forecast_el.inner_text().strip() if forecast_el else "") or "—"

                    previous_el = row.query_selector(".calendar__cell.calendar__previous")
                    previous = (previous_el.inner_text().strip() if previous_el else "") or "—"

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
                        "previous": previous,
                    })
                except Exception as exc:
                    log.debug(f"Row parse error: {exc}")
                    continue
        except Exception as exc:
            log.error(f"Event extraction error: {exc}", exc_info=True)

        return events

    @staticmethod
    def _parse_ff_time(time_str: str) -> Tuple[str, str]:
        """Convert ForexFactory time string (e.g. '8:30am') to 12h and 24h formats."""
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

    # ── Telegram Channel Scraping (News Moderation Flow) ──────────────────────
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
        """Send plain text to destination channel."""
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
            log.error(f"Send text error: {exc}", exc_info=True)
        return None

    async def _send_file_with_caption(
        self, file_bytes: bytes, mime: str, caption: str
    ):
        """
        Send a screenshot as an inline PHOTO with caption.
        buf.seek(0) is critical — without it Telethon reads from end of buffer (empty).
        force_document=False ensures Telegram renders it as a photo, not a file attachment.
        Falls back to text-only if send fails.
        """
        try:
            ext = mimetypes.guess_extension(mime) or ".png"
            buf = io.BytesIO(file_bytes)
            buf.name = f"calendar{ext}"
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
            log.info("Falling back to text-only.")
            return await self._send_text(caption)

    async def _send(self, text: str, image_data: Optional[bytes], image_mime: str):
        """Send news post — with image if available, text-only otherwise."""
        try:
            if image_data:
                buf = io.BytesIO(image_data)
                ext = mimetypes.guess_extension(image_mime) or ".jpg"
                buf.name = f"media{ext}"
                buf.seek(0)
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

    # ── Reminder Dispatcher (called from main.py reminder loop) ───────────────
    async def reminder_dispatcher_loop(self):
        """
        Dedicated reminder loop — checks every 60 seconds.
        Runs concurrently with the main poll loop via asyncio.gather in main.py.
        """
        log.info("🔔  Reminder dispatcher loop running …")
        while True:
            try:
                await self._check_reminders()
            except Exception as exc:
                log.error(f"Reminder dispatcher error: {exc}", exc_info=True)
            await asyncio.sleep(60)
