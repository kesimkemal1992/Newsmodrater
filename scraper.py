"""
scraper.py — Telethon scraper with Forex Factory calendar filter (USD + High)
"""

import asyncio
import io
import json
import logging
import mimetypes
import random
from datetime import datetime, timedelta
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

# ─── CONFIGURATION ──────────────────────────────────────────────────────────
WEEKLY_OUTLOOK_HOUR = 23        # 10 PM EAT (በ22፡46 ለመሞከር ከፈለጉ ቀጥሎ ያለውን መስመር ያንቁ)
WEEKLY_OUTLOOK_MINUTE = 29    # (ለሙከራ) ይህን አስተያየት አውጥተው ከላይ ያለውን ሰዓት 22 አድርገው ይጠቀሙ

PLAYWRIGHT_TIMEOUT_MS = 60000   # 60 seconds
PLAYWRIGHT_EXTRA_WAIT_MS = 5000 # 5 seconds

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

def _is_image(msg) -> bool:
    if isinstance(msg.media, MessageMediaPhoto):
        return True
    if isinstance(msg.media, MessageMediaDocument):
        return msg.media.document.mime_type in _IMG_MIMES
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


class ChannelScraper:
    def __init__(self, config: dict, ai_engine: AIEngine, memory: MemoryManager):
        self._cfg = config
        self._ai = ai_engine
        self._mem = memory

        self._dest_channels = config.get("dest_channels", [])
        if not self._dest_channels:
            single = config.get("dest_channel", "")
            if single:
                self._dest_channels = [single]
        if not self._dest_channels:
            raise ValueError("No destination channels configured.")

        log.info(f"📤 Posting to {len(self._dest_channels)} channel(s)")

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

    # ─── multi‑channel broadcast helpers ─────────────────────────────────────
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

    async def _broadcast_media(self, text: str, img: Optional[bytes], mime: str):
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

    # ─── main poll cycle ─────────────────────────────────────────────────────
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

    # ─── daily briefing ──────────────────────────────────────────────────────
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
            log.info("No high‑impact USD events today – skip")
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
        log.info("📸 Taking screenshot (USD + High)")
        screenshot = await self._take_forex_factory_screenshot_today()
        if screenshot:
            sent = await self._broadcast_file_with_caption(screenshot, "image/png", brief)
        else:
            sent = await self._broadcast_text(brief)
        if sent:
            await self._mem.save_daily_briefing(today, sent.id, events)
            log.info(f"📅 Briefing posted id={sent.id}")

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

    # ─── reminders ───────────────────────────────────────────────────────────
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
        for dest in self._dest_channels:
            try:
                sent = await self._client.send_message(dest, safe, parse_mode="md", reply_to=reply_id)
                log.info(f"🚨 Reminder to {dest} id={sent.id}")
            except Exception as e:
                log.error(f"Reminder fail {dest}: {e}")
            await asyncio.sleep(1)
        await self._mem.mark_reminder_sent(key)
        await self._mem.increment_reminder_count(today)

    # ─── weekly outlook (with real calendar filter) ──────────────────────────
    async def _check_weekly_outlook(self):
        now = _eat_now()
        if now.weekday() != 6:   # Sunday
            return
        if now.hour != WEEKLY_OUTLOOK_HOUR:
            return
        # Optional minute check (uncomment for precise testing at 22:46)
        # if now.minute != 46:
        #     return

        week_key = now.strftime("%Y-%W")
        if self._weekly_posted_date == week_key:
            return
        log.info("📆 Weekly outlook – scraping ForexFactory with filter (USD + High)")
        events = await self._scrape_forex_factory_week()
        if not events:
            log.info("No USD High events this week – skip")
            self._weekly_posted_date = week_key
            return

        start = now + timedelta(days=1)
        end = start + timedelta(days=4)
        week_range = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"
        text = await self._ai.generate_weekly_outlook(events, week_range)
        if not text:
            log.error("Weekly outlook text failed")
            return

        log.info("📸 Taking weekly screenshot (USD + High via real filter)")
        screenshot = await self._take_forex_factory_screenshot_week()
        if screenshot:
            sent = await self._broadcast_file_with_caption(screenshot, "image/png", text)
        else:
            sent = await self._broadcast_text(text)
        if sent:
            self._weekly_posted_date = week_key
            log.info(f"📆 Weekly outlook posted id={sent.id}")

    # ─── ForexFactory scraping + screenshot (using real filter) ──────────────
    async def _scrape_forex_factory_today(self) -> List[dict]:
        return await self._playwright_scrape_today()

    async def _scrape_forex_factory_week(self) -> List[dict]:
        return await self._playwright_scrape_week()

    async def _take_forex_factory_screenshot_today(self) -> Optional[bytes]:
        return await self._playwright_screenshot_today()

    async def _take_forex_factory_screenshot_week(self) -> Optional[bytes]:
        return await self._playwright_screenshot_week()

    # ─── real calendar filter (click on filter button, select USD + High) ────
    async def _apply_forexfactory_filter(self, page):
        """Open filter dialog, select USD currency and High impact, then apply."""
        try:
            # Click the filter button (icon with class .filter__open)
            filter_btn = await page.query_selector(".filter__open")
            if filter_btn:
                await filter_btn.click()
                await page.wait_for_timeout(1500)
            else:
                log.warning("Filter button not found, try alternative selector")
                filter_btn = await page.query_selector("button[aria-label='Calendar filters']")
                if filter_btn:
                    await filter_btn.click()
                    await page.wait_for_timeout(1500)

            # Select currency USD (checkbox or dropdown)
            # ForexFactory uses a list of checkboxes: #filter-currency li input[value='USD']
            usd_check = await page.query_selector("#filter-currency input[value='USD']")
            if usd_check:
                is_checked = await usd_check.get_property("checked")
                if not is_checked:
                    await usd_check.click()
                    log.info("USD currency selected")
            else:
                log.warning("USD checkbox not found – using fallback filter")

            # Select impact: High (value='high') – uncheck others if needed
            high_check = await page.query_selector("#filter-impact input[value='high']")
            if high_check:
                is_checked = await high_check.get_property("checked")
                if not is_checked:
                    await high_check.click()
                    log.info("High impact selected")
            else:
                log.warning("High impact checkbox not found")

            # Click apply button
            apply_btn = await page.query_selector(".filter__apply")
            if apply_btn:
                await apply_btn.click()
                await page.wait_for_timeout(3000)  # wait for refresh
            else:
                log.warning("Apply button not found, closing with ESC")
                await page.keyboard.press("Escape")

        except Exception as e:
            log.error(f"Filter application failed: {e}")

    # ─── playwright scrapers (with real filter) ──────────────────────────────
    async def _playwright_scrape_today(self) -> List[dict]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                ctx = await browser.new_context(locale="en-US", timezone_id="Africa/Addis_Ababa")
                page = await ctx.new_page()
                await page.goto("https://www.forexfactory.com/calendar?day=today", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=PLAYWRIGHT_TIMEOUT_MS)
                await page.wait_for_timeout(2000)

                # apply real filter (USD + High)
                await self._apply_forexfactory_filter(page)

                # wait for table to refresh
                await page.wait_for_selector(".calendar__table", timeout=10000)
                await page.wait_for_timeout(PLAYWRIGHT_EXTRA_WAIT_MS)

                events = await self._extract_events_from_page(page, strict_usd=False)  # filter already applied
                await browser.close()
                red = [e for e in events if e.get("impact") == "red"]
                log.info(f"Today scrape (filtered): {len(events)} events, {len(red)} red")
                return red   # return only red events
        except Exception as e:
            log.error(f"Playwright today failed: {e}", exc_info=True)
            return []

    async def _playwright_scrape_week(self) -> List[dict]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                ctx = await browser.new_context(locale="en-US", timezone_id="Africa/Addis_Ababa")
                page = await ctx.new_page()
                await page.goto("https://www.forexfactory.com/calendar", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=PLAYWRIGHT_TIMEOUT_MS)
                await page.wait_for_timeout(2000)

                # apply real filter (USD + High)
                await self._apply_forexfactory_filter(page)

                # wait for refresh and scroll to load all rows
                await page.wait_for_selector(".calendar__table", timeout=10000)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(3000)

                events = await self._extract_events_from_page(page, strict_usd=False)
                await browser.close()
                red = [e for e in events if e.get("impact") == "red"]
                log.info(f"Week scrape (filtered): {len(events)} events, {len(red)} red")
                if red:
                    log.info(f"Sample: {[e.get('name') for e in red[:5]]}")
                return red
        except Exception as e:
            log.error(f"Playwright week failed: {e}", exc_info=True)
            return []

    # ─── screenshot with real filter ─────────────────────────────────────────
    async def _playwright_screenshot_today(self) -> Optional[bytes]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                ctx = await browser.new_context(locale="en-US", timezone_id="Africa/Addis_Ababa", viewport={"width": 1280, "height": 1000})
                page = await ctx.new_page()
                await page.goto("https://www.forexfactory.com/calendar?day=today", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=PLAYWRIGHT_TIMEOUT_MS)
                await page.wait_for_timeout(2000)

                await self._apply_forexfactory_filter(page)

                await page.wait_for_selector(".calendar__table", timeout=10000)
                await page.wait_for_timeout(2000)

                # optional: hide day breaker rows
                await page.evaluate("document.querySelectorAll('.calendar__row--day-breaker').forEach(r => r.style.display='none')")

                table = await page.query_selector(".calendar__table")
                screenshot = await table.screenshot(type="png") if table else await page.screenshot(clip={"x":0,"y":0,"width":1280,"height":1000}, type="png")
                await browser.close()
                log.info(f"Today screenshot: {len(screenshot)} bytes")
                return screenshot
        except Exception as e:
            log.error(f"Screenshot today failed: {e}", exc_info=True)
            return None

    async def _playwright_screenshot_week(self) -> Optional[bytes]:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                ctx = await browser.new_context(locale="en-US", timezone_id="Africa/Addis_Ababa", viewport={"width": 1280, "height": 1800})
                page = await ctx.new_page()
                await page.goto("https://www.forexfactory.com/calendar", timeout=30000)
                await page.wait_for_selector(".calendar__table", timeout=PLAYWRIGHT_TIMEOUT_MS)
                await page.wait_for_timeout(2000)

                await self._apply_forexfactory_filter(page)

                await page.wait_for_selector(".calendar__table", timeout=10000)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(2000)

                table = await page.query_selector(".calendar__table")
                screenshot = await table.screenshot(type="png") if table else await page.screenshot(full_page=True, type="png")
                await browser.close()
                log.info(f"Week screenshot: {len(screenshot)} bytes")
                return screenshot
        except Exception as e:
            log.error(f"Screenshot week failed: {e}", exc_info=True)
            return None

    # ─── event extraction (no extra USD filter if already filtered) ─────────
    async def _extract_events_from_page(self, page, strict_usd: bool = True) -> List[dict]:
        events = []
        current_date = ""
        try:
            rows = await page.query_selector_all(".calendar__row--event")
            log.debug(f"Found {len(rows)} event rows")
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
                        "previous": previous
                    })
                except Exception as ex:
                    log.debug(f"Row parse error: {ex}")
        except Exception as ex:
            log.error(f"Extraction error: {ex}", exc_info=True)
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

    # ─── message processing (unchanged from original) ────────────────────────
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
