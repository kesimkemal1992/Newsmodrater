"""
scraper.py — Telethon scraper with Forex Factory XML data + Playwright screenshots
Production mode: TEST_MODE=False (default) sends real posts.
Set environment variable TEST_MODE=true to simulate without sending.
"""

import asyncio
import io
import json
import logging
import mimetypes
import os
import random
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple

import pytz
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

from ai_engine import AIEngine
from memory import MemoryManager
from forexfactory_xml import fetch_and_filter_events

log = logging.getLogger("scraper")

EAT = pytz.timezone("Africa/Addis_Ababa")
_IMG_MIMES = {"image/jpeg", "image/png", "image/webp", "image/gif"}

# ─── CONFIGURATION ──────────────────────────────────────────────────────────
WEEKLY_OUTLOOK_HOUR = 22        # 10:00 PM EAT (Sunday)
WEEKLY_OUTLOOK_MINUTE = 10      # 10 minutes past the hour

PLAYWRIGHT_TIMEOUT_MS = 60000   # 60 seconds
PLAYWRIGHT_EXTRA_WAIT_MS = 5000 # 5 seconds

# Test mode from environment (default False = real posting)
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
if TEST_MODE:
    log.warning("🧪 TEST_MODE is ENABLED — no real posts will be sent.")

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

# ─── Priority keywords for reminders ────────────────────────────────────────
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
        self._test_mode = TEST_MODE  # from environment

        self._dest_channels = config.get("dest_channels", [])
        if not self._dest_channels:
            single = config.get("dest_channel", "")
            if single:
                self._dest_channels = [single]
        if not self._dest_channels:
            raise ValueError("No destination channels configured.")
        log.info(f"📤 Posting to {len(self._dest_channels)} channel(s) | test_mode={self._test_mode}")

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
            log.info("Using StringSession.")
        else:
            self._client = TelegramClient(config.get("session_name", "manager_session"), config["api_id"], config["api_hash"])
            log.info("Using file session.")

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

    # ─── Broadcast helpers with test mode support ────────────────────────────
    async def _broadcast_text(self, text: str):
        if self._test_mode:
            log.info(f"🧪 TEST MODE: Would send text to {self._dest_channels}:\n{text[:500]}...")
            return None

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
        if self._test_mode:
            log.info(f"🧪 TEST MODE: Would send screenshot ({len(data)} bytes) with caption:\n{caption[:500]}...")
            return None

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

    async def _broadcast_media(self, text: str, img: Optional[bytes], mime: str):
        if self._test_mode:
            log.info(f"🧪 TEST MODE: Would send media: text_len={len(text)}, img_size={len(img) if img else 0}")
            return None

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

    # ─── Main poll cycle ─────────────────────────────────────────────────────
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
                log.error(f"Error on {ch}: {e}", exc_info=True)
                await asyncio.sleep(5)

    # ─── Daily briefing (uses XML for events) ────────────────────────────────
    async def _check_daily_briefing(self):
        now = _eat_now()
        today = _eat_today_str()
        if not (7 <= now.hour < 9):
            return
        if await self._mem.has_daily_briefing(today):
            return
        log.info(f"📅 Daily briefing time {today}")
        events = await self._scrape_forex_factory_today()
        if not events:
            log.info("No USD High events today – skip")
            await self._mem.save_daily_briefing(today, -1, [])
            return
        self._todays_events = events
        self._todays_vip_events = self._select_vip_events(events)
        log.info(f"VIP slots: {[e.get('name') for e in self._todays_vip_events]}")
        date_display = now.strftime("%A, %B %d, %Y")
        brief = await self._ai.generate_daily_briefing(events, date_display)
        if not brief:
            log.error("Briefing generation failed")
            return
        log.info("📸 Taking today's screenshot (USD+High)")
        screenshot = await self._take_forex_factory_screenshot_today()
        if screenshot:
            sent = await self._broadcast_file_with_caption(screenshot, "image/png", brief)
        else:
            sent = await self._broadcast_text(brief)
        if sent and not self._test_mode:
            await self._mem.save_daily_briefing(today, sent.id, events)
            log.info(f"📅 Briefing posted id={sent.id}")

    # ─── VIP event selection (unchanged) ─────────────────────────────────────
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

    # ─── Reminders ───────────────────────────────────────────────────────────
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
            log.error("Alert generation failed")
            return
        safe = _clean_caption(alert)
        if self._test_mode:
            log.info(f"🧪 TEST MODE: Would send reminder to {self._dest_channels}:\n{safe[:300]}...")
            await self._mem.mark_reminder_sent(key)
            await self._mem.increment_reminder_count(today)
            return
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(dest, safe, parse_mode="md", reply_to=reply_id)
                log.info(f"🚨 Reminder to {dest} id={sent.id}")
            except Exception as e:
                log.error(f"Reminder fail {dest}: {e}")
            await asyncio.sleep(1)
        await self._mem.mark_reminder_sent(key)
        await self._mem.increment_reminder_count(today)

    # ─── WEEKLY OUTLOOK (XML + screenshot) ───────────────────────────────────
    async def _check_weekly_outlook(self):
        now = _eat_now()
        if not self._test_mode:
            if now.weekday() != 6:
                return
            if now.hour != WEEKLY_OUTLOOK_HOUR or now.minute != WEEKLY_OUTLOOK_MINUTE:
                return
        else:
            log.info("🧪 TEST MODE: Weekly outlook will run now regardless of time.")
            # optional: add a small delay to avoid loop? not needed.

        week_key = now.strftime("%Y-%W")
        if self._weekly_posted_date == week_key and not self._test_mode:
            return

        log.info("📆 Weekly outlook – fetching USD High events via XML")
        events = await self._scrape_forex_factory_week()
        if not events:
            log.info("No USD High events this week – skip")
            self._weekly_posted_date = week_key
            return

        week_start = now + timedelta(days=1)
        week_end = week_start + timedelta(days=4)
        week_range = f"{week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}"
        caption = await self._ai.generate_weekly_outlook(events, week_range)
        if not caption:
            caption = self._fallback_weekly(events, week_range)

        log.info("📸 Taking weekly screenshot (USD+High via JS filter)")
        screenshot = await self._take_forex_factory_screenshot_week()
        if screenshot:
            sent = await self._broadcast_file_with_caption(screenshot, "image/png", caption)
        else:
            sent = await self._broadcast_text(caption)

        if sent and not self._test_mode:
            self._weekly_posted_date = week_key
            log.info(f"📆 Weekly outlook posted id={sent.id}")

    # ─── Data scraping: XML first, fallback to Playwright ────────────────────
    async def _scrape_forex_factory_week(self) -> List[dict]:
        try:
            loop = asyncio.get_event_loop()
            xml_events = await loop.run_in_executor(
                None,
                lambda: fetch_and_filter_events(currency="USD", impact="High")
            )
            if xml_events:
                log.info(f"Week XML: {len(xml_events)} USD High events found")
                return xml_events
        except Exception as e:
            log.warning(f"XML failed: {e}, falling back to Playwright")
        return await self._playwright_scrape_week_fallback()

    async def _scrape_forex_factory_today(self) -> List[dict]:
        try:
            loop = asyncio.get_event_loop()
            xml_events = await loop.run_in_executor(
                None,
                lambda: fetch_and_filter_events(currency="USD", impact="High")
            )
            if xml_events:
                log.info(f"Today XML: {len(xml_events)} USD High events found")
                return xml_events
        except Exception as e:
            log.warning(f"XML failed: {e}, falling back to Playwright")
        return await self._playwright_scrape_today_fallback()

    async def _playwright_scrape_week_fallback(self) -> List[dict]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox"]
                )
                context = await browser.new_context(locale="en-US", timezone_id="Africa/Addis_Ababa")
                page = await context.new_page()
                await page.goto("https://www.forexfactory.com/calendar", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=30000)
                await page.wait_for_timeout(5000)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(2000)
                events = await self._extract_events_from_page(page, strict_usd=True)
                await browser.close()
                red = [e for e in events if e.get("impact") == "red"]
                log.info(f"Week Playwright fallback: {len(red)} USD High events")
                return red
        except Exception as e:
            log.error(f"Playwright fallback failed: {e}")
            return []

    async def _playwright_scrape_today_fallback(self) -> List[dict]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox"]
                )
                context = await browser.new_context(locale="en-US", timezone_id="Africa/Addis_Ababa")
                page = await context.new_page()
                await page.goto("https://www.forexfactory.com/calendar?day=today", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=30000)
                await page.wait_for_timeout(5000)
                events = await self._extract_events_from_page(page, strict_usd=True)
                await browser.close()
                red = [e for e in events if e.get("impact") == "red"]
                log.info(f"Today Playwright fallback: {len(red)} USD High events")
                return red
        except Exception as e:
            log.error(f"Today Playwright fallback failed: {e}")
            return []

    # ─── Screenshot with JavaScript filter (USD + High only) ─────────────────
    async def _take_forex_factory_screenshot_week(self) -> Optional[bytes]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox"]
                )
                context = await browser.new_context(
                    locale="en-US",
                    timezone_id="Africa/Addis_Ababa",
                    viewport={"width": 1280, "height": 1800}
                )
                page = await context.new_page()
                await page.goto("https://www.forexfactory.com/calendar", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=30000)
                await page.wait_for_timeout(5000)
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
                        } else { hide = true; }
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
                log.info(f"Week screenshot captured: {len(screenshot)} bytes")
                return screenshot
        except Exception as e:
            log.error(f"Week screenshot failed: {e}")
            return None

    async def _take_forex_factory_screenshot_today(self) -> Optional[bytes]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox"]
                )
                context = await browser.new_context(
                    locale="en-US",
                    timezone_id="Africa/Addis_Ababa",
                    viewport={"width": 1280, "height": 1000}
                )
                page = await context.new_page()
                await page.goto("https://www.forexfactory.com/calendar?day=today", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=30000)
                await page.wait_for_timeout(5000)
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
                        } else { hide = true; }
                        if (hide) row.style.display = 'none';
                    });
                    document.querySelectorAll('.calendar__row--day-breaker').forEach(r => r.style.display = 'none');
                """)
                table = await page.query_selector(".calendar__table")
                screenshot = await table.screenshot(type="png") if table else await page.screenshot(clip={"x":0,"y":0,"width":1280,"height":1000}, type="png")
                await browser.close()
                log.info(f"Today screenshot captured: {len(screenshot)} bytes")
                return screenshot
        except Exception as e:
            log.error(f"Today screenshot failed: {e}")
            return None

    # ─── Event extraction from page (for fallback) ───────────────────────────
    async def _extract_events_from_page(self, page, strict_usd: bool = True) -> List[dict]:
        events = []
        current_date = ""
        try:
            rows = await page.query_selector_all(".calendar__row--event")
            for row in rows:
                try:
                    date_cell = await row.query_selector(".calendar__cell.calendar__date")
                    if date_cell:
                        dt = (await date_cell.inner_text()).strip()
                        if dt:
                            current_date = dt
                    impact_el = await row.query_selector(".calendar__impact span")
                    if not impact_el:
                        continue
                    iclass = await impact_el.get_attribute("class") or ""
                    itext = (await impact_el.inner_text()).strip().lower()
                    if "high" in iclass or "impact--high" in iclass or itext == "high":
                        impact = "red"
                    elif "medium" in iclass or "impact--medium" in iclass or itext == "medium":
                        impact = "orange"
                    else:
                        continue
                    curr_el = await row.query_selector(".calendar__cell.calendar__currency")
                    currency = (await curr_el.inner_text()).strip() if curr_el else ""
                    if strict_usd and currency.upper() != "USD":
                        continue
                    time_el = await row.query_selector(".calendar__cell.calendar__time")
                    time_raw = (await time_el.inner_text()).strip() if time_el else ""
                    name_el = await row.query_selector(".calendar__cell.calendar__event")
                    name = (await name_el.inner_text()).strip() if name_el else "Unknown"
                    fcast_el = await row.query_selector(".calendar__cell.calendar__forecast")
                    forecast = ((await fcast_el.inner_text()).strip() if fcast_el else "") or "—"
                    prev_el = await row.query_selector(".calendar__cell.calendar__previous")
                    previous = ((await prev_el.inner_text()).strip() if prev_el else "") or "—"
                    t12, t24 = self._parse_ff_time(time_raw)
                    events.append({
                        "date": current_date,
                        "time_raw": time_raw,
                        "time_12h": t12,
                        "time_24h": t24,
                        "currency": currency,
                        "name": name,
                        "impact": impact,
                        "forecast": forecast,
                        "previous": previous,
                    })
                except Exception:
                    continue
        except Exception as e:
            log.error(f"Extraction error: {e}")
        return events

    @staticmethod
    def _parse_ff_time(t: str) -> Tuple[str, str]:
        if not t or t in ("All Day", "Tentative", ""):
            return ("All Day", "")
        try:
            clean = t.replace("\u202f", " ").strip().lower()
            dt = datetime.strptime(clean, "%I:%M%p")
            return (dt.strftime("%I:%M %p"), dt.strftime("%H:%M"))
        except:
            try:
                dt = datetime.strptime(t.strip().lower(), "%I%p")
                return (dt.strftime("%I:%M %p"), dt.strftime("%H:%M"))
            except:
                return (t, "")

    @staticmethod
    def _fallback_weekly(events: list, week_range: str) -> str:
        from itertools import groupby
        lines = [f"📅 WEEKLY HIGH IMPACT\nWeek of {week_range}\n"]
        for day, day_events in groupby(events, key=lambda e: e.get("date", "Unknown")):
            lines.append(f"\n{day}")
            for ev in day_events:
                forecast = ev.get('forecast', '').strip()
                previous = ev.get('previous', '').strip()
                detail = ""
                if (forecast and forecast != '—') or (previous and previous != '—'):
                    f_disp = forecast if forecast and forecast != '—' else ''
                    p_disp = previous if previous and previous != '—' else ''
                    if f_disp and p_disp:
                        detail = f"  ↳ Forecast: {f_disp} | Previous: {p_disp}"
                    elif f_disp:
                        detail = f"  ↳ Forecast: {f_disp}"
                    elif p_disp:
                        detail = f"  ↳ Previous: {p_disp}"
                lines.append(f"🔴 {ev.get('time_12h', '—')} | USD: {ev.get('name', 'Unknown')}")
                if detail:
                    lines.append(detail)
        lines.append("\n📌 NOTE:\nMonitor all USD Red events closely. Manage risk carefully around high-impact releases.")
        return "\n".join(lines)

    # ─── Telegram channel processing ─────────────────────────────────────────
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
        if sent and not self._test_mode:
            await self._mem.log_posted(src, msg.id, sent.id, chash, verdict, post)
            log.info(f"✅ Posted id={sent.id} engine={verdict.get('engine')}")

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
