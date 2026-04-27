"""
scraper.py — Telethon channel scraper, forwarder, and Forex Factory scheduler.

CLOUDFLARE FIX:
  ForexFactory blocks headless Chromium with a Cloudflare challenge page.
  Solution: Use the FF public JSON/iCal API endpoints which are NOT protected
  by Cloudflare, combined with a requests session with realistic headers.

  Data source priority:
    1. FF public API  → https://nfs.faireconomy.media/ff_calendar_thisweek.json
       (community mirror of FF data — same source, no Cloudflare)
    2. FF iCal feed   → https://www.forexfactory.com/ffcal_week_this.xml
    3. Screenshot     → Still uses Playwright but with full stealth mode
       (playwright-stealth package) for the visual capture only

  Install requirements:
    pip install playwright-stealth requests

Timezone: Africa/Addis_Ababa (GMT+3) — EAT.
"""

import asyncio
import io
import json
import logging
import mimetypes
import os
import random
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

import pytz
import requests
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
TEST_MODE: bool = os.environ.get("TEST_MODE", "false").strip().lower() == "true"
if TEST_MODE:
    log.warning("⚠️  TEST_MODE=true — all time/date guards bypassed.")

# ── PIL availability ───────────────────────────────────────────────────────────
try:
    from PIL import Image as _PIL_Image
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_AVAILABLE = False
    log.warning("PIL not installed — perceptual hash dedup disabled.")

# ── Reminder config ────────────────────────────────────────────────────────────
_BONUS_HOUR = 18

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

# ── HTTP session for API calls ─────────────────────────────────────────────────
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.6367.155 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.forexfactory.com/",
}

# ── FF public data sources (no Cloudflare) ─────────────────────────────────────
_FF_JSON_THIS_WEEK = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
_FF_JSON_NEXT_WEEK = "https://nfs.faireconomy.media/ff_calendar_nextweek.json"
_FF_ICAL_THIS_WEEK = "https://www.forexfactory.com/ffcal_week_this.xml"

# ── Playwright stealth args ────────────────────────────────────────────────────
_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-blink-features=AutomationControlled",
    "--disable-infobars",
    "--window-size=1280,900",
    "--start-maximized",
]

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.6367.155 Safari/537.36"
)

_JS_STEALTH = """
    Object.defineProperty(navigator, 'webdriver',  { get: () => undefined });
    Object.defineProperty(navigator, 'plugins',    { get: () => [1,2,3,4,5] });
    Object.defineProperty(navigator, 'languages',  { get: () => ['en-US','en'] });
    Object.defineProperty(navigator, 'platform',   { get: () => 'Win32' });
    window.chrome = { runtime: {} };
"""

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


# ── Helper functions ───────────────────────────────────────────────────────────

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


# ── FF Data Fetcher (Cloudflare-free) ─────────────────────────────────────────

def _fetch_ff_json(url: str) -> List[dict]:
    """
    Fetch FF calendar data from the public JSON mirror.
    Returns normalised event dicts matching our internal format.
    """
    try:
        resp = requests.get(url, headers=_HTTP_HEADERS, timeout=20)
        resp.raise_for_status()
        raw_events = resp.json()
        log.info(f"FF JSON API returned {len(raw_events)} events from {url}")
        return _normalise_ff_json(raw_events)
    except Exception as exc:
        log.error(f"FF JSON fetch failed ({url}): {exc}")
        return []


def _normalise_ff_json(raw: list) -> List[dict]:
    """
    Convert FF JSON API format to our internal event dict format.

    FF JSON fields:
      title, country, date, impact, forecast, previous
      date format: "2025-04-28T14:30:00-0400"  (ET/New York time)
    """
    events = []
    for item in raw:
        try:
            country = item.get("country", "").upper()
            impact_raw = item.get("impact", "").lower()

            # Map impact
            if impact_raw == "high":
                impact = "red"
            elif impact_raw == "medium":
                impact = "orange"
            else:
                continue  # skip low/holiday

            # Parse date — FF JSON uses US Eastern time
            date_str = item.get("date", "")
            if not date_str:
                continue

            # Parse ISO datetime with offset
            try:
                # Handle offset like -0400 or +0000
                dt_et = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except Exception:
                try:
                    # fallback: strip offset and parse naively
                    dt_et = datetime.strptime(date_str[:19], "%Y-%m-%dT%H:%M:%S")
                    dt_et = dt_et.replace(tzinfo=timezone.utc)
                except Exception:
                    log.debug(f"Cannot parse date: {date_str}")
                    continue

            # Convert to EAT
            dt_eat = dt_et.astimezone(EAT)

            time_12h = dt_eat.strftime("%I:%M %p")
            time_24h = dt_eat.strftime("%H:%M")
            date_display = dt_eat.strftime("%A %b %d")

            forecast = item.get("forecast", "") or "—"
            previous = item.get("previous", "") or "—"
            if not forecast.strip():
                forecast = "—"
            if not previous.strip():
                previous = "—"

            events.append({
                "date": date_display,
                "time_raw": time_12h,
                "time_12h": time_12h,
                "time_24h": time_24h,
                "currency": country,
                "name": item.get("title", "Unknown"),
                "impact": impact,
                "forecast": forecast,
                "previous": previous,
            })
        except Exception as exc:
            log.debug(f"Event parse error: {exc} | raw={item}")
    return events


def _fetch_ff_today(all_events: List[dict]) -> List[dict]:
    """Filter events to today EAT date only."""
    today = _eat_now().strftime("%Y-%m-%d")
    result = []
    for ev in all_events:
        # Reconstruct full date from time_24h + today for comparison
        time_24h = ev.get("time_24h", "")
        if not time_24h:
            continue
        try:
            # Check if this event's EAT date matches today
            # We stored date_display as "Monday Apr 28" — compare via time_24h
            now_eat = _eat_now()
            event_hour = int(time_24h.split(":")[0])
            event_min = int(time_24h.split(":")[1])
            # Build candidate datetime for today
            candidate = now_eat.replace(hour=event_hour, minute=event_min, second=0, microsecond=0)
            # Check date_display string contains today's day number
            day_num = str(now_eat.day)
            if day_num in ev.get("date", ""):
                result.append(ev)
        except Exception:
            continue
    return result


# ── Playwright screenshot (stealth mode) ──────────────────────────────────────

def _playwright_screenshot(url: str, viewport_height: int = 1000) -> Optional[bytes]:
    """
    Take a screenshot of FF calendar using stealth Playwright.
    Applies playwright-stealth if available, otherwise uses manual JS patches.
    Shows USD Red rows only via JS filter.
    """
    try:
        from playwright.sync_api import sync_playwright

        # Try to import playwright_stealth
        try:
            from playwright_stealth import stealth_sync
            _stealth_available = True
        except ImportError:
            _stealth_available = False
            log.warning("playwright-stealth not installed — using manual JS stealth.")

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=_LAUNCH_ARGS,
            )
            context = browser.new_context(
                locale="en-US",
                timezone_id="Africa/Addis_Ababa",
                viewport={"width": 1280, "height": viewport_height},
                user_agent=_USER_AGENT,
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )

            # Always inject JS stealth patches
            context.add_init_script(_JS_STEALTH)

            page = context.new_page()

            # Apply playwright-stealth if available
            if _stealth_available:
                stealth_sync(page)
                log.info("playwright-stealth applied.")

            log.info(f"Navigating to {url} for screenshot …")
            page.goto(url, timeout=45_000, wait_until="domcontentloaded")

            # Wait for Cloudflare to clear (up to 15 seconds)
            for attempt in range(15):
                page.wait_for_timeout(1000)
                title = page.title()
                if "just a moment" not in title.lower():
                    log.info(f"Page loaded after {attempt + 1}s: '{title}'")
                    break
                log.debug(f"Waiting for Cloudflare… attempt {attempt + 1}/15")
            else:
                log.error("Cloudflare challenge not resolved after 15s — screenshot skipped.")
                browser.close()
                return None

            # Now wait for calendar table
            try:
                page.wait_for_selector(".calendar__table", timeout=15_000)
            except Exception:
                log.error("Calendar table not found after Cloudflare cleared.")
                browser.close()
                return None

            # Apply USD Red filter
            page.evaluate(_JS_FILTER_USD_RED)
            page.wait_for_timeout(500)

            # Screenshot table element
            table = page.query_selector(".calendar__table")
            if table:
                screenshot_bytes = table.screenshot(type="png")
                log.info(f"Screenshot captured: {len(screenshot_bytes):,} bytes")
            else:
                log.warning("Table element not found — using viewport.")
                screenshot_bytes = page.screenshot(
                    clip={"x": 0, "y": 0, "width": 1280, "height": viewport_height},
                    type="png"
                )

            browser.close()
            return screenshot_bytes

    except Exception as exc:
        log.error(f"Playwright screenshot failed: {exc}", exc_info=True)
        return None


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
        self._test_briefing_done: bool = False
        self._test_weekly_done: bool = False

        session_string = config.get("session_string", "").strip()
        if session_string:
            session = StringSession(session_string)
        else:
            session = config.get("session_name", "manager_session")
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
            log.warning("Reconnecting …")
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

    # ── Broadcast helpers ──────────────────────────────────────────────────────

    async def _broadcast_text(self, text: str):
        sent = None
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(dest, text, parse_mode="md")
                log.info(f"  → Text sent to {dest} | msg_id={sent.id}")
            except ChatWriteForbiddenError:
                log.error(f"❌  Cannot post to {dest}")
            except FloodWaitError as fwe:
                await asyncio.sleep(fwe.seconds + 3)
                try:
                    sent = await self._client.send_message(dest, text, parse_mode="md")
                except Exception as exc:
                    log.error(f"Retry failed: {exc}")
            except Exception as exc:
                log.error(f"Send text error: {exc}", exc_info=True)
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
                log.error(f"Send file error: {exc}", exc_info=True)
                try:
                    sent = await self._client.send_message(dest, caption, parse_mode="md")
                except Exception as exc2:
                    log.error(f"Text fallback failed: {exc2}")
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
                log.error(f"❌  Cannot post to {dest}")
            except FloodWaitError as fwe:
                await asyncio.sleep(fwe.seconds + 3)
                try:
                    sent = await self._client.send_message(dest, text, parse_mode="md")
                except Exception as exc:
                    log.error(f"Retry failed: {exc}")
            except Exception as exc:
                log.error(f"Send error: {exc}", exc_info=True)
            await asyncio.sleep(1)
        return sent

    # ── Main poll cycle ────────────────────────────────────────────────────────

    async def poll_and_forward(self):
        stats = await self._mem.stats()
        log.info(
            f"Poll cycle | sources={len(self._sources)} | "
            f"hashes={stats['tracked_hashes']} | test_mode={TEST_MODE}"
        )
        if not await self._ensure_connected():
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
                log.error(f"Error on {channel}: {exc}", exc_info=True)
                await asyncio.sleep(5)

    # ── Daily Briefing ─────────────────────────────────────────────────────────

    async def _check_daily_briefing(self):
        now = _eat_now()
        today_str = _eat_today_str()

        if TEST_MODE:
            if self._test_briefing_done:
                return
            log.info("🧪  TEST_MODE: running daily briefing immediately.")
            try:
                await self._mem._db.execute(
                    "DELETE FROM daily_briefings WHERE date_str=?", (today_str,)
                )
                await self._mem._db.commit()
            except Exception as exc:
                log.warning(f"Could not clear briefing record: {exc}")
        else:
            if not (7 <= now.hour < 9):
                return
            if await self._mem.has_daily_briefing(today_str):
                return

        log.info(f"📅  Fetching FF data for {today_str} via JSON API …")
        all_week_events = await self._fetch_ff_events_this_week()
        log.info(f"Total events fetched: {len(all_week_events)}")

        # Filter to today
        today_events = _fetch_ff_today(all_week_events)
        log.info(f"Today's events: {len(today_events)}")

        # USD Red only
        usd_red = [
            e for e in today_events
            if e.get("currency", "").upper() == "USD" and e.get("impact") == "red"
        ]
        log.info(f"USD Red today: {len(usd_red)}")

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
            log.error("Failed to generate briefing text.")
            return

        log.info("📸  Taking FF screenshot (USD Red only) …")
        screenshot = await self._take_screenshot_today()

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
        log.info("📆  Fetching FF data for the week …")

        all_events = await self._fetch_ff_events_this_week()
        usd_red = [
            e for e in all_events
            if e.get("currency", "").upper() == "USD" and e.get("impact") == "red"
        ]
        log.info(f"USD Red this week: {len(usd_red)}")

        if not usd_red:
            log.info("No USD Red events this week — skipping.")
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

        screenshot = await self._take_screenshot_week()
        if screenshot:
            sent = await self._broadcast_file_with_caption(screenshot, "image/png", outlook_text)
        else:
            sent = await self._broadcast_text(outlook_text)

        if sent:
            self._weekly_posted_date = week_key
            if TEST_MODE:
                self._test_weekly_done = True
            log.info(f"📆  Weekly outlook posted → msg_id={sent.id}")

    # ── VIP Slot Selection ─────────────────────────────────────────────────────

    def _select_vip_events(self, events: List[dict]) -> List[dict]:
        eligible = [e for e in events if _is_reminder_eligible(e)]
        if not eligible:
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
        log.info(f"⏰  Reminder: {event.get('name')}")
        mot_index = await self._mem.get_and_increment_motivational_index()
        alert_text = await self._ai.generate_alert(event, motivational_index=mot_index)
        if not alert_text:
            return
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(
                    dest, alert_text, parse_mode="md", reply_to=reply_to_msg_id
                )
                if sent:
                    log.info(f"🚨  Reminder sent to {dest} → msg_id={sent.id}")
            except Exception as exc:
                log.error(f"Reminder failed for {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        await self._mem.mark_reminder_sent(event_key)
        await self._mem.increment_reminder_count(today_str)

    # ── FF Data Fetch (Cloudflare-free JSON API) ───────────────────────────────

    async def _fetch_ff_events_this_week(self) -> List[dict]:
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: _fetch_ff_json(_FF_JSON_THIS_WEEK)
        )

    async def _fetch_ff_events_next_week(self) -> List[dict]:
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: _fetch_ff_json(_FF_JSON_NEXT_WEEK)
        )

    # ── Screenshots (Playwright stealth) ──────────────────────────────────────

    async def _take_screenshot_today(self) -> Optional[bytes]:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: _playwright_screenshot(
                "https://www.forexfactory.com/calendar?day=today",
                viewport_height=1000,
            )
        )

    async def _take_screenshot_week(self) -> Optional[bytes]:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: _playwright_screenshot(
                "https://www.forexfactory.com/calendar",
                viewport_height=1800,
            )
        )

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
            log.info(f"[SKIP] Duplicate — {content_hash[:12]}…")
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
            if any(kw in text.lower() for kw in [
                "today's high impact", "daily briefing", "high impact news",
                "forexfactory", "forex factory", "economic calendar",
            ]):
                log.info(f"[SKIP] Briefing already posted — duplicate from source channel.")
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
        log.info(f"✅  Posted → msg_id={sent.id}")

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
                log.debug(f"Typing skipped: {exc}")

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
