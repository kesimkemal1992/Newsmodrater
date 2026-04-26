"""
scraper.py — Telethon channel scraper, forwarder, and Forex Factory scheduler.

TEST MODE:
  Set environment variable TEST_MODE=true on Railway to bypass all time checks.
  The bot will immediately scrape FF, post daily briefing, and run reminders
  as if it were 7 AM on a Monday — useful for verifying deployment works.

  Set TEST_MODE=false (or remove it) to return to normal scheduled behaviour.

Timezone: Africa/Addis_Ababa (GMT+3) — EAT.
"""

import asyncio
import io
import json
import logging
import mimetypes
import os
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

# ── TEST MODE ──────────────────────────────────────────────────────────────────
# Set TEST_MODE=true in Railway environment variables to bypass time checks.
# Briefing + reminders fire immediately on every poll cycle when enabled.
TEST_MODE: bool = os.environ.get("TEST_MODE", "false").strip().lower() == "true"

if TEST_MODE:
    log.warning("⚠️  TEST_MODE=true — all time/date guards bypassed. Disable after testing!")

# ── PIL availability ───────────────────────────────────────────────────────────
try:
    from PIL import Image as _PIL_Image
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_AVAILABLE = False
    log.warning("PIL not installed — perceptual hash dedup disabled. Run: pip install Pillow")

# ── Reminder config ────────────────────────────────────────────────────────────
_BONUS_HOUR = 18  # 06:00 PM EAT

_ALWAYS_BONUS_KEYWORDS = (
    "nfp", "non-farm payroll", "non-farm payrolls",
    "cpi", "consumer price index", "core cpi",
)

_TOP_TIER_KEYWORDS = (
    "fomc", "federal open market committee", "federal reserve",
    "fed rate", "rate decision", "interest rate decision",
    "fed chair", "powell speaks", "jerome powell",
    "fomc statement", "fomc minutes", "fomc meeting", "fomc press",
)

_PRIORITY_KEYWORDS = [
    "fomc", "federal open market committee",
    "interest rate decision", "rate decision",
    "nfp", "non-farm payroll", "non-farm payrolls",
    "cpi", "consumer price index",
    "pce", "core pce", "gdp",
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

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.6367.155 Safari/537.36"
)

_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-blink-features=AutomationControlled",
    "--window-size=1280,900",
]

_JS_FILTER_USD_RED = """
(function() {
    let lastCurrency = '';
    document.querySelectorAll('.calendar__row--event').forEach(row => {
        const curEl = row.querySelector('.calendar__cell.calendar__currency');
        if (curEl) {
            const cur = curEl.innerText.trim();
            if (cur) lastCurrency = cur;
        }
        const impEl = row.querySelector('.calendar__impact span');
        const impact = impEl ? impEl.className : '';
        if (lastCurrency !== 'USD' || !impact.includes('high')) {
            row.style.display = 'none';
        }
    });
    document.querySelectorAll('.calendar__row--day-breaker').forEach(breaker => {
        let next = breaker.nextElementSibling;
        let hasVisible = false;
        while (next && !next.classList.contains('calendar__row--day-breaker')) {
            if (next.style.display !== 'none') { hasVisible = true; break; }
            next = next.nextElementSibling;
        }
        if (!hasVisible) breaker.style.display = 'none';
    });
})();
"""


# ── Helpers ────────────────────────────────────────────────────────────────────

def _is_fomc_event(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in _FOMC_KEYWORDS)

def _is_always_bonus(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in _ALWAYS_BONUS_KEYWORDS)

def _is_top_tier(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in _TOP_TIER_KEYWORDS)

def _is_priority_event(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in _PRIORITY_KEYWORDS)

def _event_hour_eat(event: dict) -> int:
    t = event.get("time_24h", "")
    if not t:
        return 0
    try:
        return int(t.split(":")[0])
    except (ValueError, IndexError):
        return 0

def _qualifies_for_bonus(event: dict) -> bool:
    name = event.get("name", "")
    if _is_always_bonus(name):
        return True
    if _is_top_tier(name) and _event_hour_eat(event) >= _BONUS_HOUR:
        return True
    return False

def _is_reminder_eligible(event: dict) -> bool:
    currency = event.get("currency", "").upper().strip()
    name_lower = event.get("name", "").lower()
    if event.get("impact") != "red":
        return False
    if _is_fomc_event(event.get("name", "")):
        return currency == "USD" or any(kw in name_lower for kw in _GOLD_KEYWORDS)
    is_usd = currency == "USD"
    is_gold = any(kw in name_lower for kw in _GOLD_KEYWORDS)
    if not (is_usd or is_gold):
        return False
    forecast = event.get("forecast", "").strip()
    previous = event.get("previous", "").strip()
    return bool(forecast and forecast != "—") and bool(previous and previous != "—")

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

def _phash_image(image_bytes: bytes, hash_size: int = 8) -> Optional[str]:
    if not _PIL_AVAILABLE:
        return None
    try:
        img = _PIL_Image.open(io.BytesIO(image_bytes)).convert("L")
        img = img.resize((hash_size, hash_size), _PIL_Image.LANCZOS)
        pixels = list(img.getdata())
        avg = sum(pixels) / len(pixels)
        bits = "".join("1" if p >= avg else "0" for p in pixels)
        return f"{int(bits, 2):0{hash_size * hash_size // 4}x}"
    except Exception as exc:
        log.debug(f"phash failed: {exc}")
        return None

def _phash_distance(h1: str, h2: str) -> int:
    try:
        return bin(int(h1, 16) ^ int(h2, 16)).count("1")
    except Exception:
        return 999

def _is_forex_factory_image(image_bytes: bytes) -> bool:
    if not _PIL_AVAILABLE:
        return False
    try:
        img = _PIL_Image.open(io.BytesIO(image_bytes)).convert("RGB")
        img = img.resize((64, 64))
        pixels = list(img.getdata())
        light = sum(1 for r, g, b in pixels if r > 200 and g > 200 and b > 185)
        return (light / len(pixels)) > 0.4
    except Exception:
        return False

def _make_browser_context(playwright, viewport_height: int = 1000):
    browser = playwright.chromium.launch(headless=True, args=_LAUNCH_ARGS)
    context = browser.new_context(
        locale="en-US",
        timezone_id="Africa/Addis_Ababa",
        viewport={"width": 1280, "height": viewport_height},
        user_agent=_USER_AGENT,
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        },
    )
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    """)
    return browser, context

def _wait_for_calendar(page, url: str, timeout: int = 45_000) -> bool:
    log.debug(f"Navigating to: {url}")
    page.goto(url, timeout=timeout, wait_until="domcontentloaded")
    page.wait_for_timeout(2000)
    try:
        page.wait_for_selector(".calendar__table", timeout=20_000)
        log.debug("Calendar table found.")
        return True
    except Exception:
        pass
    try:
        page.wait_for_selector("table.calendar", timeout=10_000)
        log.debug("Calendar table found (fallback selector).")
        return True
    except Exception:
        pass
    title = page.title()
    current_url = page.url
    log.error(
        f"Calendar table NOT found. title='{title}' url='{current_url}' "
        f"— possible Cloudflare block or FF layout change."
    )
    try:
        log.debug(f"Page HTML snippet: {page.content()[:500]}")
    except Exception:
        pass
    return False


# ── Main class ─────────────────────────────────────────────────────────────────

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
        log.info(f"🧪  TEST_MODE = {TEST_MODE}")

        self._sources = config["source_channels"]
        self._min_delay = config["min_delay_seconds"]
        self._max_delay = config["max_delay_seconds"]
        self._lookback_hours = config["lookback_hours"]

        self._todays_events: List[dict] = []
        self._todays_vip_events: List[dict] = []
        self._weekly_posted_date: Optional[str] = None
        self._today_screenshot_phash: Optional[str] = None
        self._today_screenshot_date: Optional[str] = None

        # In TEST_MODE track if briefing already ran this process session
        self._test_briefing_done: bool = False
        self._test_weekly_done: bool = False

        session_string = config.get("session_string", "").strip()
        if session_string:
            session = StringSession(session_string)
            log.info("Using StringSession for authentication.")
        else:
            session = config.get("session_name", "manager_session")
            log.info(f"Using file session: {session}.session")

        self._client = TelegramClient(session, config["api_id"], config["api_hash"])

    # ── Lifecycle ──────────────────────────────────────────────────────────────

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
                log.info("✅  Reconnected.")
            except Exception as exc:
                log.error(f"Reconnect failed: {exc}")
                return False
        return True

    # ── Broadcast helpers ──────────────────────────────────────────────────────

    async def _broadcast_text(self, text: str):
        sent = None
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(dest, text, parse_mode="md")
                log.info(f"  → Text sent to {dest} | msg_id={sent.id}")
            except ChatWriteForbiddenError:
                log.error(f"❌  Cannot post to {dest} — check admin rights.")
            except FloodWaitError as fwe:
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
                log.error(f"Send file error on {dest}: {exc}", exc_info=True)
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
                log.error(f"❌  Cannot post to {dest} — check admin rights.")
            except FloodWaitError as fwe:
                await asyncio.sleep(fwe.seconds + 3)
                try:
                    sent = await self._client.send_message(dest, text, parse_mode="md")
                except Exception as exc:
                    log.error(f"Retry failed for {dest}: {exc}")
            except Exception as exc:
                log.error(f"Send error on {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        return sent

    # ── Main poll cycle ────────────────────────────────────────────────────────

    async def poll_and_forward(self):
        stats = await self._mem.stats()
        log.info(
            f"Poll cycle | sources={len(self._sources)} | "
            f"hashes={stats['tracked_hashes']} | "
            f"posted_24h={stats['posted_last_24h']} | "
            f"test_mode={TEST_MODE}"
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
                await asyncio.sleep(fwe.seconds + 5)
            except Exception as exc:
                log.error(f"Error on channel {channel}: {exc}", exc_info=True)
                await asyncio.sleep(5)

    # ── Daily Briefing ─────────────────────────────────────────────────────────

    async def _check_daily_briefing(self):
        now = _eat_now()
        today_str = _eat_today_str()

        if TEST_MODE:
            # In test mode: run once per process session, skip DB check
            if self._test_briefing_done:
                return
            log.info("🧪  TEST_MODE: bypassing time and date guards for daily briefing.")
            # Clear today's briefing record so it re-posts cleanly
            try:
                await self._mem._db.execute(
                    "DELETE FROM daily_briefings WHERE date_str=?", (today_str,)
                )
                await self._mem._db.commit()
            except Exception as exc:
                log.warning(f"Could not clear briefing record: {exc}")
        else:
            # Normal mode: only run between 7 AM and 9 AM
            if not (7 <= now.hour < 9):
                return
            if await self._mem.has_daily_briefing(today_str):
                return

        log.info(f"📅  Scraping ForexFactory for {today_str} …")
        events = await self._scrape_forex_factory_today()
        log.info(f"Scrape returned {len(events)} total events.")

        usd_red = [
            e for e in events
            if e.get("currency", "").upper() == "USD" and e.get("impact") == "red"
        ]
        log.info(f"USD Red events: {len(usd_red)}")

        if not usd_red:
            log.info("No USD Red events today — skipping daily briefing.")
            await self._mem.save_daily_briefing(today_str, -1, [])
            if TEST_MODE:
                self._test_briefing_done = True
            return

        for ev in usd_red:
            log.info(f"  ✅ {ev.get('time_12h')} | {ev.get('name')} | F:{ev.get('forecast')} P:{ev.get('previous')}")

        self._todays_events = usd_red
        self._todays_vip_events = self._select_vip_events(usd_red)

        date_display = now.strftime("%A, %B %d, %Y")
        briefing_text = await self._ai.generate_daily_briefing(usd_red, date_display)
        if not briefing_text:
            log.error("Failed to generate daily briefing text.")
            return

        log.info("📸  Taking ForexFactory screenshot (USD Red only) …")
        screenshot = await self._take_forex_factory_screenshot_today()

        if screenshot:
            log.info(f"Screenshot OK: {len(screenshot):,} bytes")
            phash = _phash_image(screenshot)
            if phash:
                self._today_screenshot_phash = phash
                self._today_screenshot_date = today_str
            sent = await self._broadcast_file_with_caption(screenshot, "image/png", briefing_text)
        else:
            log.warning("Screenshot failed — sending text only.")
            sent = await self._broadcast_text(briefing_text)

        if sent:
            await self._mem.save_daily_briefing(today_str, sent.id, usd_red)
            log.info(f"📅  Daily briefing posted → msg_id={sent.id}")
            if TEST_MODE:
                self._test_briefing_done = True
        else:
            log.error("Failed to send daily briefing.")

    # ── VIP Slot Selection ─────────────────────────────────────────────────────

    def _select_vip_events(self, events: List[dict]) -> List[dict]:
        eligible = [e for e in events if _is_reminder_eligible(e)]
        if not eligible:
            log.info("No reminder-eligible USD Red events today.")
            return []

        def sort_key(e):
            return (0 if _is_priority_event(e.get("name", "")) else 1, e.get("time_24h", "99:99"))

        sorted_eligible = sorted(eligible, key=sort_key)
        slot1 = sorted_eligible[0]
        vip = [slot1]
        log.info(f"💥 Slot 1: {slot1.get('name')} at {slot1.get('time_12h')}")

        for event in sorted_eligible[1:]:
            if _qualifies_for_bonus(event):
                vip.append(event)
                log.info(f"💥 Slot 2 BONUS: {event.get('name')} at {event.get('time_12h')}")
                break

        return vip

    # ── Reminder Scheduler ─────────────────────────────────────────────────────

    async def _check_reminders(self):
        today_str = _eat_today_str()
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

        for i, event in enumerate(vip_events):
            event_key = f"{today_str}_{event.get('name', '')}_{event.get('currency', '')}"
            if i == 1 and not _qualifies_for_bonus(event):
                continue
            if await self._mem.has_reminder_been_sent(event_key):
                continue

            event_time_str = event.get("time_24h", "")
            if not event_time_str:
                continue

            if TEST_MODE:
                # In test mode fire the reminder immediately — no time window check
                log.info(f"🧪  TEST_MODE: firing reminder immediately for {event.get('name')}")
                await self._send_reminder(event, event_key, briefing_msg_id, today_str)
                await asyncio.sleep(2)
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
                await asyncio.sleep(2)

    async def _send_reminder(self, event: dict, event_key: str, reply_to_msg_id: int, today_str: str):
        log.info(f"⏰  Sending 10-min reminder for: {event.get('name')}")
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
                log.error(f"Failed to send reminder to {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        await self._mem.mark_reminder_sent(event_key)
        await self._mem.increment_reminder_count(today_str)

    # ── Weekly Outlook ─────────────────────────────────────────────────────────

    async def _check_weekly_outlook(self):
        now = _eat_now()

        if TEST_MODE:
            if self._test_weekly_done:
                return
            log.info("🧪  TEST_MODE: running weekly outlook immediately.")
        else:
            if now.weekday() != 6 or now.hour != 21:
                return
            week_key = now.strftime("%Y-%W")
            if self._weekly_posted_date == week_key:
                return

        week_key = now.strftime("%Y-%W")

        log.info("📆  Scraping ForexFactory for the week …")
        events = await self._scrape_forex_factory_week()
        usd_red = [
            e for e in events
            if e.get("currency", "").upper() == "USD" and e.get("impact") == "red"
        ]
        if not usd_red:
            log.info("No USD Red events this week — skipping weekly outlook.")
            self._weekly_posted_date = week_key
            if TEST_MODE:
                self._test_weekly_done = True
            return

        week_start = now + timedelta(days=1)
        week_end = week_start + timedelta(days=4)
        week_range = f"{week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}"
        outlook_text = await self._ai.generate_weekly_outlook(usd_red, week_range)
        if not outlook_text:
            return

        screenshot = await self._take_forex_factory_screenshot_week()
        if screenshot:
            sent = await self._broadcast_file_with_caption(screenshot, "image/png", outlook_text)
        else:
            sent = await self._broadcast_text(outlook_text)

        if sent:
            self._weekly_posted_date = week_key
            if TEST_MODE:
                self._test_weekly_done = True
            log.info(f"📆  Weekly outlook posted → msg_id={sent.id}")

    # ── ForexFactory Async Wrappers ────────────────────────────────────────────

    async def _scrape_forex_factory_today(self) -> List[dict]:
        return await asyncio.get_event_loop().run_in_executor(None, self._playwright_scrape_today)

    async def _scrape_forex_factory_week(self) -> List[dict]:
        return await asyncio.get_event_loop().run_in_executor(None, self._playwright_scrape_week)

    async def _take_forex_factory_screenshot_today(self) -> Optional[bytes]:
        return await asyncio.get_event_loop().run_in_executor(None, self._playwright_screenshot_today)

    async def _take_forex_factory_screenshot_week(self) -> Optional[bytes]:
        return await asyncio.get_event_loop().run_in_executor(None, self._playwright_screenshot_week)

    # ── Playwright: Scrape Today ───────────────────────────────────────────────

    def _playwright_scrape_today(self) -> List[dict]:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser, context = _make_browser_context(p)
                page = context.new_page()
                ok = _wait_for_calendar(page, "https://www.forexfactory.com/calendar?day=today")
                if not ok:
                    browser.close()
                    return []
                events = self._extract_events_from_page(page)
                browser.close()
                return events
        except Exception as exc:
            log.error(f"Playwright today scrape failed: {exc}", exc_info=True)
            return []

    # ── Playwright: Scrape Week ────────────────────────────────────────────────

    def _playwright_scrape_week(self) -> List[dict]:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser, context = _make_browser_context(p)
                page = context.new_page()
                ok = _wait_for_calendar(page, "https://www.forexfactory.com/calendar")
                if not ok:
                    browser.close()
                    return []
                events = self._extract_events_from_page(page)
                browser.close()
                return events
        except Exception as exc:
            log.error(f"Playwright week scrape failed: {exc}", exc_info=True)
            return []

    # ── Playwright: Today Screenshot ───────────────────────────────────────────

    def _playwright_screenshot_today(self) -> Optional[bytes]:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser, context = _make_browser_context(p, viewport_height=1000)
                page = context.new_page()
                ok = _wait_for_calendar(page, "https://www.forexfactory.com/calendar?day=today")
                if not ok:
                    browser.close()
                    return None
                page.evaluate(_JS_FILTER_USD_RED)
                page.wait_for_timeout(500)
                table = page.query_selector(".calendar__table")
                if table:
                    screenshot_bytes = table.screenshot(type="png")
                else:
                    log.warning("Table not found — using viewport screenshot.")
                    screenshot_bytes = page.screenshot(
                        clip={"x": 0, "y": 0, "width": 1280, "height": 1000}, type="png"
                    )
                browser.close()
                log.info(f"Today screenshot: {len(screenshot_bytes):,} bytes")
                return screenshot_bytes
        except Exception as exc:
            log.error(f"Playwright today screenshot failed: {exc}", exc_info=True)
            return None

    # ── Playwright: Week Screenshot ────────────────────────────────────────────

    def _playwright_screenshot_week(self) -> Optional[bytes]:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser, context = _make_browser_context(p, viewport_height=1800)
                page = context.new_page()
                ok = _wait_for_calendar(page, "https://www.forexfactory.com/calendar")
                if not ok:
                    browser.close()
                    return None
                page.evaluate(_JS_FILTER_USD_RED)
                page.wait_for_timeout(500)
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
        events = []
        current_date = ""
        last_currency = ""
        try:
            rows = page.query_selector_all(".calendar__row--event")
            log.debug(f"Found {len(rows)} calendar rows.")
            for row in rows:
                try:
                    date_cell = row.query_selector(".calendar__cell.calendar__date")
                    if date_cell:
                        dt = date_cell.inner_text().strip()
                        if dt:
                            current_date = dt
                    impact_el = row.query_selector(".calendar__impact span")
                    if not impact_el:
                        continue
                    impact_class = impact_el.get_attribute("class") or ""
                    if "high" in impact_class:
                        impact = "red"
                    elif "medium" in impact_class:
                        impact = "orange"
                    else:
                        continue
                    currency_el = row.query_selector(".calendar__cell.calendar__currency")
                    if currency_el:
                        cur = currency_el.inner_text().strip()
                        if cur:
                            last_currency = cur
                    currency = last_currency or "—"
                    time_el = row.query_selector(".calendar__cell.calendar__time")
                    time_raw = time_el.inner_text().strip() if time_el else ""
                    event_el = row.query_selector(".calendar__cell.calendar__event")
                    event_name = event_el.inner_text().strip() if event_el else "Unknown"
                    forecast_el = row.query_selector(".calendar__cell.calendar__forecast")
                    forecast = (forecast_el.inner_text().strip() if forecast_el else "") or "—"
                    previous_el = row.query_selector(".calendar__cell.calendar__previous")
                    previous = (previous_el.inner_text().strip() if previous_el else "") or "—"
                    time_12h, time_24h = self._parse_ff_time(time_raw)
                    log.debug(f"Row: {currency} | {impact} | {time_12h} | {event_name}")
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

    # ── Telegram Channel Scraping ──────────────────────────────────────────────

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
                channel, limit=50, min_id=last_id if last_id else 0,
                offset_date=cutoff, reverse=True
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
            except Exception as exc:
                log.warning(f"Image download failed: {exc}")

        content_hash = self._mem.hash_combined(text, image_data)
        if await self._mem.is_duplicate(content_hash):
            log.info(f"[SKIP] Duplicate hash — {content_hash[:12]}…")
            return

        if image_data and _PIL_AVAILABLE and _is_forex_factory_image(image_data):
            today_str = _eat_today_str()
            incoming_phash = _phash_image(image_data)
            if incoming_phash and self._today_screenshot_phash and self._today_screenshot_date == today_str:
                dist = _phash_distance(incoming_phash, self._today_screenshot_phash)
                if dist <= 12:
                    log.info(f"[SKIP] Near-duplicate FF screenshot (dist={dist}).")
                    await self._mem.mark_seen(content_hash, source=source_channel)
                    return

        today_str = _eat_today_str()
        if image_data and await self._mem.has_daily_briefing(today_str):
            text_lower = text.lower()
            if any(kw in text_lower for kw in [
                "today's high impact", "daily briefing", "high impact news",
                "forexfactory", "forex factory", "economic calendar",
            ]):
                log.info(f"[SKIP] Briefing already posted — source channel duplicate.")
                await self._mem.mark_seen(content_hash, source=source_channel)
                return

        log.info(f"🔍  Analysing msg {msg.id} from {source_channel}")
        verdict = await self._ai.analyse(text, image_data, image_mime)
        await self._mem.mark_seen(content_hash, source=source_channel)

        if not verdict.get("approved"):
            log.info(f"[REJECTED] {verdict.get('reason')}")
            return

        post_text = self._build_post(verdict)
        await asyncio.sleep(random.uniform(self._min_delay, self._max_delay))
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
        log.info(f"✅  Posted → msg_id={sent.id} | engine={verdict.get('engine')}")

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
