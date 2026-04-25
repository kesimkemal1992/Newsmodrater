"""
main.py — AXIOM INTEL Telegram Manager
Institutional Senior Trader Edition

Architecture:
  • Poll loop     — scrapes Telegram sources every POLL_INTERVAL seconds
  • Reminder loop — checks for upcoming events every 60s (separate coroutine)
  • Both run concurrently via asyncio.gather

Setup:
    python generate_session.py   ← run locally once, get SESSION_STRING
    Paste SESSION_STRING into Railway environment variables.
    Set GEMINI_API_KEY, GROQ_API_KEY, SOURCE_CHANNELS, DEST_CHANNEL.
    Deploy. Done.

Optional env vars:
    CALENDAR_SOURCE=forex_factory   — enable ForexFactory daily briefings
    POLL_INTERVAL=60                — seconds between Telegram scrape cycles
"""

import asyncio
import logging
import os
import signal
import sys

from dotenv import load_dotenv

load_dotenv()

from scraper import ChannelScraper
from ai_engine import AIEngine
from memory import MemoryManager

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("main")


# ─── Config helpers ────────────────────────────────────────────────────────────
def _require(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        log.error(f"❌  Missing required env var: {key}")
        sys.exit(1)
    return val


# ─── Config ────────────────────────────────────────────────────────────────────
CONFIG = {
    # Telegram credentials
    "api_id":         int(_require("TELEGRAM_API_ID")),
    "api_hash":       _require("TELEGRAM_API_HASH"),
    "phone":          os.getenv("TELEGRAM_PHONE", ""),

    # Session (StringSession preferred — no OTP on server)
    "session_string": os.getenv("SESSION_STRING", ""),
    "session_name":   os.getenv("SESSION_NAME", "manager_session"),

    # Channels
    "source_channels": [
        c.strip() for c in _require("SOURCE_CHANNELS").split(",") if c.strip()
    ],
    "dest_channel": _require("DEST_CHANNEL"),

    # AI keys
    "gemini_api_key": _require("GEMINI_API_KEY"),
    "groq_api_key":   _require("GROQ_API_KEY"),

    # Channel focus (injected into every AI moderation prompt)
    "channel_category": os.getenv(
        "CHANNEL_CATEGORY",
        "Geopolitical events (wars, sanctions, elections), Central Bank policy "
        "(FED, ECB, BOE, BOJ), Macroeconomic data (CPI, NFP, GDP, PCE), "
        "Gold (XAU) safe-haven flows, Oil (WTI/Brent) supply disruptions, "
        "Major FX pairs (EURUSD, GBPUSD, USDJPY, DXY). "
        "NO trading signals. NO technical-only charts.",
    ),

    # ── Calendar feature ──────────────────────────────────────────────────────
    # Set to "forex_factory" to enable daily briefings, reminders, weekly outlook.
    # Leave empty to disable calendar features.
    "calendar_source": os.getenv("CALENDAR_SOURCE", ""),

    # Timing
    "poll_interval_seconds": int(os.getenv("POLL_INTERVAL", "60")),
    "min_delay_seconds":     float(os.getenv("MIN_DELAY", "8")),
    "max_delay_seconds":     float(os.getenv("MAX_DELAY", "30")),
    "lookback_hours":        int(os.getenv("LOOKBACK_HOURS", "2")),

    # Memory / dedup
    "db_path":       os.getenv("DB_PATH", "memory.db"),
    "hash_ttl_days": int(os.getenv("HASH_TTL_DAYS", "30")),
}


# ─── Graceful shutdown ─────────────────────────────────────────────────────────
_shutdown = asyncio.Event()


def _handle_signal(sig, _frame):
    log.info(f"Signal {sig.name} received — shutting down …")
    _shutdown.set()


signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ─── Poll loop ─────────────────────────────────────────────────────────────────
async def poll_loop(scraper: ChannelScraper):
    log.info("✅  Telegram client connected. Entering poll loop …")
    interval = CONFIG["poll_interval_seconds"]
    while not _shutdown.is_set():
        try:
            await scraper.poll_and_forward()
        except Exception as exc:
            log.error(f"Poll cycle error: {exc}", exc_info=True)

        try:
            await asyncio.wait_for(_shutdown.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass  # Normal — loop again


# ─── Reminder dispatcher loop ──────────────────────────────────────────────────
async def reminder_loop(scraper: ChannelScraper):
    """
    Runs every 60 seconds, independently of the main poll loop.
    Checks if any ForexFactory event is 10 minutes away and sends alerts.
    """
    log.info("🔔  Reminder dispatcher started.")
    while not _shutdown.is_set():
        try:
            await scraper._check_daily_briefing()
            await scraper._check_reminders()
            await scraper._check_weekly_outlook()
        except Exception as exc:
            log.error(f"Reminder loop error: {exc}", exc_info=True)

        try:
            await asyncio.wait_for(_shutdown.wait(), timeout=60)
        except asyncio.TimeoutError:
            pass
    log.info("🔔  Reminder dispatcher stopped.")


# ─── Main entry point ──────────────────────────────────────────────────────────
async def run():
    log.info("🚀  AXIOM INTEL — Geopolitical Channel Manager starting …")
    log.info(f"📡  Monitoring {len(CONFIG['source_channels'])} news source(s)")
    log.info(f"📤  Destination: {CONFIG['dest_channel']}")

    cal = CONFIG["calendar_source"]
    if cal:
        log.info(f"📅  Calendar source: {cal} (daily briefings + reminders enabled)")
    else:
        log.info("📅  Calendar source: not configured (set CALENDAR_SOURCE to enable)")

    if CONFIG["session_string"]:
        log.info("🔑  Auth mode: StringSession ✅")
    else:
        log.info("🔑  Auth mode: File session (ensure .session file exists)")

    # ── Init memory ────────────────────────────────────────────────────────────
    memory = MemoryManager(
        db_path=CONFIG["db_path"],
        ttl_days=CONFIG["hash_ttl_days"],
    )
    await memory.init()

    # ── Init AI engine ─────────────────────────────────────────────────────────
    ai = AIEngine(
        gemini_key=CONFIG["gemini_api_key"],
        groq_key=CONFIG["groq_api_key"],
        channel_category=CONFIG["channel_category"],
    )

    # ── Init scraper ───────────────────────────────────────────────────────────
    scraper = ChannelScraper(
        config=CONFIG,
        ai_engine=ai,
        memory=memory,
    )
    await scraper.start()

    try:
        if CONFIG["calendar_source"]:
            # Run poll loop + reminder dispatcher concurrently
            await asyncio.gather(
                poll_loop(scraper),
                reminder_loop(scraper),
            )
        else:
            # Calendar disabled — poll loop only
            await poll_loop(scraper)
    finally:
        log.info("🛑  Shutting down gracefully …")
        await scraper.stop()
        await memory.close()
        log.info("👋  Goodbye.")


if __name__ == "__main__":
    asyncio.run(run())
