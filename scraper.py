"""
scraper.py — Telethon channel scraper, forwarder, and Forex Factory scheduler.

CLOUDFLARE BYPASS SOLUTION (2026):
  Data   → FF public JSON API (no Cloudflare, instant)
  Screenshot → nodriver (best Cloudflare evasion for Python in 2026)
             → Falls back to Pillow-generated image if nodriver fails

  nodriver patches Chrome at binary level — navigator.webdriver=undefined,
  realistic TLS fingerprints, no automation flags. Cloudflare cannot detect it.

  Install:
    pip install nodriver requests Pillow

  On Railway nixpacks — add to nixpacks.toml or Dockerfile:
    apt-get install -y chromium chromium-driver

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

# ── PIL ────────────────────────────────────────────────────────────────────────
try:
    from PIL import Image, ImageDraw, ImageFont
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_AVAILABLE = False
    log.warning("Pillow not installed — fallback image generation disabled.")

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

# ── HTTP / FF API ──────────────────────────────────────────────────────────────
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.6367.155 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
# Primary + mirror sources — tried in order until one succeeds
_FF_JSON_THIS_WEEK_SOURCES = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
    "https://cdn-nfs.faireconomy.media/ff_calendar_thisweek.json",
    "https://raw.githubusercontent.com/mahalanobisforex/forex-factory-json/main/thisweek.json",
]
_FF_JSON_NEXT_WEEK_SOURCES = [
    "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
    "https://cdn-nfs.faireconomy.media/ff_calendar_nextweek.json",
]
_FF_URL_TODAY = "https://www.forexfactory.com/calendar?day=today"
_FF_URL_WEEK  = "https://www.forexfactory.com/calendar"

# ── JS: filter table to USD Red only ──────────────────────────────────────────
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
    // Remove header/footer/nav noise
    ['header','footer','nav','.site-nav','.site-footer',
     '.site-header','.calendar__filters'].forEach(sel => {
        document.querySelectorAll(sel).forEach(el => el.style.display = 'none');
    });
})();
"""

# ── Image design ───────────────────────────────────────────────────────────────
_IMG_WIDTH     = 900
_ROW_HEIGHT    = 62
_HEADER_HEIGHT = 78
_FOOTER_HEIGHT = 46
_PADDING       = 22

_COLOR_BG        = (15, 20, 30)
_COLOR_HEADER_BG = (20, 28, 45)
_COLOR_ROW_ALT   = (22, 30, 48)
_COLOR_ROW_MAIN  = (18, 24, 38)
_COLOR_RED       = (220, 50, 50)
_COLOR_ACCENT    = (255, 200, 60)
_COLOR_TEXT      = (220, 225, 235)
_COLOR_SUBTEXT   = (140, 150, 170)
_COLOR_DIVIDER   = (35, 45, 65)
_COLOR_FORECAST  = (100, 200, 140)
_COLOR_PREVIOUS  = (160, 170, 190)
_COLOR_FOOTER_BG = (12, 16, 26)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _is_fomc_event(name: str) -> bool:
    return any(kw in name.lower() for kw in _FOMC_KEYWORDS)

def _is_always_bonus(name: str) -> bool:
    return any(kw in name.lower() for kw in _ALWAYS_BONUS_KEYWORDS)

def _is_top_tier(name: str) -> bool:
    return any(kw in name.lower() for kw in _TOP_TIER_KEYWORDS)

def _is_priority_event(name: str) -> bool:
    return any(kw in name.lower() for kw in _PRIORITY_KEYWORDS)

def _event_hour_eat(event: dict) -> int:
    t = event.get("time_24h", "")
    try:
        return int(t.split(":")[0])
    except Exception:
        return 0

def _qualifies_for_bonus(event: dict) -> bool:
    name = event.get("name", "")
    if _is_always_bonus(name):
        return True
    return _is_top_tier(name) and _event_hour_eat(event) >= _BONUS_HOUR

def _is_reminder_eligible(event: dict) -> bool:
    currency   = event.get("currency", "").upper().strip()
    name_lower = event.get("name", "").lower()
    if event.get("impact") != "red":
        return False
    if _is_fomc_event(event.get("name", "")):
        return currency == "USD" or any(kw in name_lower for kw in _GOLD_KEYWORDS)
    if currency != "USD" and not any(kw in name_lower for kw in _GOLD_KEYWORDS):
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
        img = Image.open(io.BytesIO(image_bytes)).convert("L")
        img = img.resize((hash_size, hash_size), Image.LANCZOS)
        pixels = list(img.getdata())
        avg = sum(pixels) / len(pixels)
        bits = "".join("1" if p >= avg else "0" for p in pixels)
        return f"{int(bits, 2):0{hash_size * hash_size // 4}x}"
    except Exception:
        return None

def _phash_distance(h1: str, h2: str) -> int:
    try:
        return bin(int(h1, 16) ^ int(h2, 16)).count("1")
    except Exception:
        return 999


# ── FF JSON fetcher — retry + mirror fallback ──────────────────────────────────

import time as _time

def _fetch_ff_json_url(url: str, retries: int = 3) -> Optional[list]:
    """Fetch raw JSON from one URL with exponential backoff on 429/5xx."""
    for attempt in range(1, retries + 1):
        try:
            if attempt > 1:
                wait = (2 ** attempt) + random.uniform(0, 2)
                log.info(f"Retry {attempt}/{retries} for {url} in {wait:.1f}s …")
                _time.sleep(wait)

            resp = requests.get(url, headers=_HTTP_HEADERS, timeout=25)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 15))
                log.warning(f"429 rate limit on {url} — waiting {retry_after}s …")
                _time.sleep(retry_after + 2)
                continue

            resp.raise_for_status()
            raw = resp.json()
            log.info(f"FF JSON OK: {len(raw)} events from {url}")
            return raw

        except Exception as exc:
            log.warning(f"Attempt {attempt} failed ({url}): {exc}")

    return None


def _fetch_ff_json(sources: List[str]) -> List[dict]:
    """Try each mirror URL in order until one returns data."""
    for url in sources:
        log.info(f"Trying FF source: {url}")
        raw = _fetch_ff_json_url(url)
        if raw is not None:
            return _normalise_ff_json(raw)
        log.warning("Source failed — trying next mirror …")
    log.error("All FF JSON sources failed.")
    return []

def _normalise_ff_json(raw: list) -> List[dict]:
    events = []
    for item in raw:
        try:
            country  = item.get("country", "").upper()
            impact_r = item.get("impact", "").lower()
            if impact_r == "high":
                impact = "red"
            elif impact_r == "medium":
                impact = "orange"
            else:
                continue
            date_str = item.get("date", "")
            if not date_str:
                continue
            try:
                dt_utc = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except Exception:
                dt_utc = datetime.strptime(date_str[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            dt_eat   = dt_utc.astimezone(EAT)
            time_12h = dt_eat.strftime("%I:%M %p")
            time_24h = dt_eat.strftime("%H:%M")
            date_key = dt_eat.strftime("%Y-%m-%d")
            date_disp= dt_eat.strftime("%A, %b %d")
            forecast = (item.get("forecast") or "—").strip() or "—"
            previous = (item.get("previous") or "—").strip() or "—"
            events.append({
                "date_key":  date_key,
                "date":      date_disp,
                "time_12h":  time_12h,
                "time_24h":  time_24h,
                "currency":  country,
                "name":      item.get("title", "Unknown"),
                "impact":    impact,
                "forecast":  forecast,
                "previous":  previous,
            })
        except Exception as exc:
            log.debug(f"Event parse error: {exc}")
    return events

def _filter_today(events: List[dict]) -> List[dict]:
    today = _eat_today_str()
    return [e for e in events if e.get("date_key") == today]


# ── nodriver screenshot (best Cloudflare bypass 2026) ─────────────────────────

async def _nodriver_screenshot(url: str, viewport_height: int = 1000) -> Optional[bytes]:
    """
    Use nodriver to take a screenshot of FF calendar.
    nodriver patches Chrome at binary level — Cloudflare cannot detect it.
    Falls back to None if nodriver not installed.
    """
    try:
        import nodriver as uc
    except ImportError:
        log.warning("nodriver not installed — skipping browser screenshot. Run: pip install nodriver")
        return None

    try:
        log.info(f"nodriver: launching Chrome for {url} …")
        browser = await uc.start(
            headless=True,
            browser_args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                f"--window-size=1280,{viewport_height}",
            ],
        )
        tab = await browser.get(url)

        # Wait for Cloudflare to clear — check title up to 20s
        for attempt in range(20):
            await asyncio.sleep(1)
            title = await tab.evaluate("document.title")
            log.debug(f"nodriver attempt {attempt+1}/20 — title: '{title}'")
            if title and "just a moment" not in title.lower():
                log.info(f"Page loaded after {attempt+1}s: '{title}'")
                break
        else:
            log.error("Cloudflare not cleared after 20s.")
            await browser.stop()
            return None

        # Wait for calendar table
        try:
            await tab.wait_for(".calendar__table", timeout=15)
            log.info("Calendar table found.")
        except Exception:
            log.error("Calendar table not found after Cloudflare cleared.")
            await browser.stop()
            return None

        # Apply USD Red filter
        await tab.evaluate(_JS_FILTER_USD_RED)
        await asyncio.sleep(0.8)

        # Screenshot the table element
        try:
            table_el = await tab.select(".calendar__table")
            screenshot_bytes = await table_el.save_screenshot()
            log.info(f"nodriver table screenshot: {len(screenshot_bytes):,} bytes")
        except Exception:
            log.warning("Table element screenshot failed — using full page.")
            screenshot_bytes = await tab.save_screenshot()
            log.info(f"nodriver full page screenshot: {len(screenshot_bytes):,} bytes")

        await browser.stop()
        return screenshot_bytes

    except Exception as exc:
        log.error(f"nodriver screenshot failed: {exc}", exc_info=True)
        try:
            await browser.stop()
        except Exception:
            pass
        return None


# ── Pillow fallback image generator ───────────────────────────────────────────

def _load_font(size: int, bold: bool = True):
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    ] if bold else [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]
    for path in paths:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()

def generate_calendar_image(events: List[dict], title: str, subtitle: str = "") -> Optional[bytes]:
    if not _PIL_AVAILABLE or not events:
        return None
    try:
        n_rows     = len(events)
        img_height = _HEADER_HEIGHT + (n_rows * _ROW_HEIGHT) + _FOOTER_HEIGHT + _PADDING
        img        = Image.new("RGB", (_IMG_WIDTH, img_height), _COLOR_BG)
        draw       = ImageDraw.Draw(img)

        f_title  = _load_font(22)
        f_sub    = _load_font(14, bold=False)
        f_col    = _load_font(12)
        f_time   = _load_font(15)
        f_name   = _load_font(14, bold=False)
        f_data   = _load_font(14)
        f_footer = _load_font(11, bold=False)

        # Header
        draw.rectangle([0, 0, _IMG_WIDTH, _HEADER_HEIGHT], fill=_COLOR_HEADER_BG)
        draw.rectangle([0, 0, 5, _HEADER_HEIGHT], fill=_COLOR_ACCENT)
        draw.text((_PADDING + 10, 12), title,    font=f_title, fill=_COLOR_ACCENT)
        draw.text((_PADDING + 10, 42), subtitle, font=f_sub,   fill=_COLOR_SUBTEXT)
        draw.text((_IMG_WIDTH - 230, 28), "USD  |  RED HIGH IMPACT", font=f_sub, fill=(180, 90, 90))
        draw.rectangle([0, _HEADER_HEIGHT - 2, _IMG_WIDTH, _HEADER_HEIGHT], fill=_COLOR_ACCENT)

        # Column headers
        col_y = _HEADER_HEIGHT + 8
        draw.text((26,  col_y), "TIME",     font=f_col, fill=_COLOR_SUBTEXT)
        draw.text((145, col_y), "EVENT",    font=f_col, fill=_COLOR_SUBTEXT)
        draw.text((580, col_y), "FORECAST", font=f_col, fill=_COLOR_SUBTEXT)
        draw.text((730, col_y), "PREVIOUS", font=f_col, fill=_COLOR_SUBTEXT)
        div_y = _HEADER_HEIGHT + 26
        draw.rectangle([_PADDING, div_y, _IMG_WIDTH - _PADDING, div_y + 1], fill=_COLOR_DIVIDER)

        row_y     = _HEADER_HEIGHT + _PADDING + 12
        seen_dates= set()

        for i, ev in enumerate(events):
            row_bg = _COLOR_ROW_ALT if i % 2 == 0 else _COLOR_ROW_MAIN
            draw.rectangle([0, row_y - 6, _IMG_WIDTH, row_y + _ROW_HEIGHT - 10], fill=row_bg)

            # Day label for weekly
            dk = ev.get("date_key", "")
            if dk and dk not in seen_dates:
                seen_dates.add(dk)
                if len(seen_dates) > 1:
                    draw.rectangle([0, row_y - 6, _IMG_WIDTH, row_y - 5], fill=_COLOR_ACCENT)
                day_label = ev.get("date", "")
                if day_label:
                    draw.text((_PADDING, row_y - 4), day_label.upper(), font=f_col, fill=_COLOR_ACCENT)
                    row_y += 20

            # Red dot
            draw.ellipse([14, row_y + 8, 24, row_y + 18], fill=_COLOR_RED)

            # Time
            draw.text((26, row_y + 4), ev.get("time_12h", "—"), font=f_time, fill=_COLOR_TEXT)

            # Event name
            name = ev.get("name", "Unknown")
            if len(name) > 43:
                name = name[:41] + "…"
            draw.text((145, row_y + 4), name, font=f_name, fill=_COLOR_TEXT)

            # Forecast
            fc = ev.get("forecast", "—")
            draw.text((580, row_y + 4), fc, font=f_data,
                      fill=_COLOR_FORECAST if fc != "—" else _COLOR_SUBTEXT)

            # Previous
            draw.text((730, row_y + 4), ev.get("previous", "—"), font=f_data, fill=_COLOR_PREVIOUS)

            # Row divider
            draw.rectangle(
                [_PADDING, row_y + _ROW_HEIGHT - 14, _IMG_WIDTH - _PADDING, row_y + _ROW_HEIGHT - 13],
                fill=_COLOR_DIVIDER,
            )
            row_y += _ROW_HEIGHT

        # Footer
        fy = img_height - _FOOTER_HEIGHT
        draw.rectangle([0, fy, _IMG_WIDTH, img_height], fill=_COLOR_FOOTER_BG)
        draw.rectangle([0, fy, _IMG_WIDTH, fy + 1], fill=_COLOR_DIVIDER)
        now_str = _eat_now().strftime("%A, %B %d, %Y  •  %I:%M %p")
        draw.text((_PADDING, fy + 14), f"Generated: {now_str}", font=f_footer, fill=_COLOR_SUBTEXT)
        draw.text((_IMG_WIDTH - 195, fy + 14), "Data: ForexFactory.com", font=f_footer, fill=_COLOR_SUBTEXT)

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        buf.seek(0)
        result = buf.getvalue()
        log.info(f"Fallback image generated: {len(result):,} bytes | {n_rows} events")
        return result
    except Exception as exc:
        log.error(f"Fallback image generation failed: {exc}", exc_info=True)
        return None


# ── Screenshot orchestrator ────────────────────────────────────────────────────

async def _take_screenshot(url: str, events: List[dict], title: str, subtitle: str, viewport_height: int = 1000) -> Optional[bytes]:
    """
    Try nodriver first (real FF screenshot).
    If that fails, generate a clean image from event data using Pillow.
    Always returns bytes — never returns None (text-only fallback is last resort).
    """
    # Attempt 1: nodriver real screenshot
    log.info("📸  Attempting nodriver screenshot …")
    result = await _nodriver_screenshot(url, viewport_height)
    if result:
        log.info("✅  nodriver screenshot succeeded.")
        return result

    # Attempt 2: Pillow-generated image
    log.warning("nodriver failed — generating image from event data …")
    result = await asyncio.get_event_loop().run_in_executor(
        None, lambda: generate_calendar_image(events, title, subtitle)
    )
    if result:
        log.info("✅  Pillow fallback image generated.")
        return result

    log.error("Both screenshot methods failed.")
    return None


# ── Main class ─────────────────────────────────────────────────────────────────

class ChannelScraper:
    def __init__(self, config: dict, ai_engine: AIEngine, memory: MemoryManager):
        self._cfg  = config
        self._ai   = ai_engine
        self._mem  = memory

        self._dest_channels: List[str] = config.get("dest_channels", [])
        if not self._dest_channels:
            single = config.get("dest_channel", "")
            if single:
                self._dest_channels = [single]
        if not self._dest_channels:
            raise ValueError("No destination channels configured.")

        log.info(f"📤  Posting to {len(self._dest_channels)} channel(s): {self._dest_channels}")
        log.info(f"🧪  TEST_MODE = {TEST_MODE}")

        self._sources        = config["source_channels"]
        self._min_delay      = config["min_delay_seconds"]
        self._max_delay      = config["max_delay_seconds"]
        self._lookback_hours = config["lookback_hours"]

        self._todays_events:      List[dict]    = []
        self._todays_vip_events:  List[dict]    = []
        self._weekly_posted_date: Optional[str] = None
        self._today_img_phash:    Optional[str] = None
        self._today_img_date:     Optional[str] = None
        self._test_briefing_done: bool          = False
        self._test_weekly_done:   bool          = False

        session_string = config.get("session_string", "").strip()
        session = StringSession(session_string) if session_string else config.get("session_name", "manager_session")
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
                log.info(f"  → File+caption sent to {dest} | msg_id={sent.id}")
            except Exception as exc:
                log.error(f"Send file error on {dest}: {exc}", exc_info=True)
                try:
                    sent = await self._client.send_message(dest, caption, parse_mode="md")
                    log.info(f"  → Text fallback sent to {dest}")
                except Exception as exc2:
                    log.error(f"Text fallback also failed: {exc2}")
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
        now       = _eat_now()
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

        log.info(f"📅  Fetching FF events for {today_str} …")
        all_events   = await self._fetch_this_week()
        today_events = _filter_today(all_events)
        usd_red      = [
            e for e in today_events
            if e.get("currency") == "USD" and e.get("impact") == "red"
        ]
        log.info(f"Total={len(all_events)} | Today={len(today_events)} | USD Red={len(usd_red)}")

        if not usd_red:
            log.info("No USD Red events today — skipping daily briefing.")
            await self._mem.save_daily_briefing(today_str, -1, [])
            if TEST_MODE:
                self._test_briefing_done = True
            return

        for ev in usd_red:
            log.info(f"  ✅ {ev['time_12h']} | {ev['name']} | F:{ev['forecast']} P:{ev['previous']}")

        self._todays_events     = usd_red
        self._todays_vip_events = self._select_vip_events(usd_red)

        date_display  = now.strftime("%A, %B %d, %Y")
        briefing_text = await self._ai.generate_daily_briefing(usd_red, date_display)
        if not briefing_text:
            log.error("Failed to generate briefing text.")
            return

        # Screenshot: try nodriver → fallback to Pillow
        image_bytes = await _take_screenshot(
            url=_FF_URL_TODAY,
            events=usd_red,
            title="📅  TODAY'S HIGH IMPACT NEWS",
            subtitle=date_display,
            viewport_height=1000,
        )

        if image_bytes:
            phash = _phash_image(image_bytes)
            if phash:
                self._today_img_phash = phash
                self._today_img_date  = today_str
            sent = await self._broadcast_file_with_caption(image_bytes, "image/png", briefing_text)
        else:
            log.warning("All image methods failed — sending text only.")
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
        now      = _eat_now()
        week_key = now.strftime("%Y-%W")

        if TEST_MODE:
            if self._test_weekly_done:
                return
            log.info("🧪  TEST_MODE: running weekly outlook immediately.")
        else:
            if now.weekday() != 6 or now.hour != 21:
                return
            if self._weekly_posted_date == week_key:
                return

        log.info("📆  Fetching FF events for the week …")
        all_events = await self._fetch_this_week()
        usd_red    = [
            e for e in all_events
            if e.get("currency") == "USD" and e.get("impact") == "red"
        ]
        log.info(f"USD Red this week: {len(usd_red)}")

        if not usd_red:
            log.info("No USD Red events this week — skipping.")
            self._weekly_posted_date = week_key
            if TEST_MODE:
                self._test_weekly_done = True
            return

        week_start = now + timedelta(days=1)
        week_end   = week_start + timedelta(days=4)
        week_range = f"{week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}"

        outlook_text = await self._ai.generate_weekly_outlook(usd_red, week_range)
        if not outlook_text:
            return

        image_bytes = await _take_screenshot(
            url=_FF_URL_WEEK,
            events=usd_red,
            title="📅  WEEKLY HIGH IMPACT OUTLOOK",
            subtitle=f"Week of {week_range}",
            viewport_height=1800,
        )

        if image_bytes:
            sent = await self._broadcast_file_with_caption(image_bytes, "image/png", outlook_text)
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
            return (0 if _is_priority_event(e.get("name","")) else 1, e.get("time_24h","99:99"))

        sorted_eligible = sorted(eligible, key=sort_key)
        slot1 = sorted_eligible[0]
        vip   = [slot1]
        log.info(f"💥 Slot 1: {slot1['name']} at {slot1['time_12h']}")

        for event in sorted_eligible[1:]:
            if _qualifies_for_bonus(event):
                vip.append(event)
                log.info(f"💥 Slot 2 BONUS: {event['name']} at {event['time_12h']}")
                break
        return vip

    # ── Reminder Scheduler ─────────────────────────────────────────────────────

    async def _check_reminders(self):
        today_str       = _eat_today_str()
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
                vip_events = self._select_vip_events(json.loads(row["events_json"]))
                self._todays_vip_events = vip_events
            if not vip_events:
                return

        now       = _eat_now()
        now_naive = now.replace(tzinfo=None)

        for i, event in enumerate(vip_events):
            event_key = f"{today_str}_{event.get('name','')}_{event.get('currency','')}"
            if i == 1 and not _qualifies_for_bonus(event):
                continue
            if await self._mem.has_reminder_been_sent(event_key):
                continue

            event_time_str = event.get("time_24h", "")
            if not event_time_str:
                continue

            if TEST_MODE:
                log.info(f"🧪  TEST_MODE: firing reminder for {event.get('name')}")
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
        mot_index  = await self._mem.get_and_increment_motivational_index()
        alert_text = await self._ai.generate_alert(event, motivational_index=mot_index)
        if not alert_text:
            return
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(
                    dest, alert_text, parse_mode="md", reply_to=reply_to_msg_id
                )
                if sent:
                    log.info(f"🚨  Reminder → {dest} | msg_id={sent.id}")
            except Exception as exc:
                log.error(f"Reminder failed for {dest}: {exc}", exc_info=True)
            await asyncio.sleep(1)
        await self._mem.mark_reminder_sent(event_key)
        await self._mem.increment_reminder_count(today_str)

    # ── FF Fetch wrappers ──────────────────────────────────────────────────────

    async def _fetch_this_week(self) -> List[dict]:
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: _fetch_ff_json(_FF_JSON_THIS_WEEK_SOURCES)
        )

    async def _fetch_next_week(self) -> List[dict]:
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: _fetch_ff_json(_FF_JSON_NEXT_WEEK_SOURCES)
        )

    # ── Telegram Channel Scraping ──────────────────────────────────────────────

    async def _process_channel(self, channel: str):
        if not await self._ensure_connected():
            return
        last_id     = await self._mem.get_last_msg_id(channel)
        cutoff      = None
        if last_id == 0:
            cutoff  = datetime.now(timezone.utc) - timedelta(hours=self._lookback_hours)
        new_last_id = last_id
        collected   = []
        try:
            async for msg in self._client.iter_messages(
                channel, limit=50,
                min_id=last_id if last_id else 0,
                offset_date=cutoff, reverse=True
            ):
                if msg.id <= last_id or not (msg.text or msg.media):
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
        text       = msg.text or msg.message or ""
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

        if image_data and _PIL_AVAILABLE and self._today_img_phash:
            today_str = _eat_today_str()
            if self._today_img_date == today_str:
                incoming_phash = _phash_image(image_data)
                if incoming_phash and _phash_distance(incoming_phash, self._today_img_phash) <= 12:
                    log.info(f"[SKIP] Near-duplicate calendar image.")
                    await self._mem.mark_seen(content_hash, source=source_channel)
                    return

        today_str = _eat_today_str()
        if image_data and await self._mem.has_daily_briefing(today_str):
            if any(kw in text.lower() for kw in [
                "today's high impact", "daily briefing", "high impact news",
                "forexfactory", "forex factory", "economic calendar",
            ]):
                log.info(f"[SKIP] Briefing already posted — source duplicate.")
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
                log.debug(f"Typing skipped: {exc}")

    async def _send_text(self, text: str):
        return await self._broadcast_text(text)

    async def _send_file_with_caption(self, file_bytes: bytes, mime: str, caption: str):
        return await self._broadcast_file_with_caption(file_bytes, mime, caption)

    async def _send(self, text: str, image_data: Optional[bytes], image_mime: str):
        return await self._broadcast_media(text, image_data, image_mime)

    async def reminder_dispatcher_loop(self):
        log.info("🔔  Reminder dispatcher running …")
        while True:
            try:
                await self._check_reminders()
            except Exception as exc:
                log.error(f"Reminder dispatcher error: {exc}", exc_info=True)
            await asyncio.sleep(60)
