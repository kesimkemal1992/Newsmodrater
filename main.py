"""
main.py — Telegram Self-Learning Channel Manager
Entry point: starts the scraper loop and handles graceful shutdown.
"""

import asyncio
import logging
import signal
import sys
from scraper import ChannelScraper
from ai_engine import AIEngine
from memory import MemoryManager

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("main")

# ─── Config — set via environment variables or .env ───────────────────────────
import os
from dotenv import load_dotenv

load_dotenv()

CONFIG = {
    # Telegram API credentials (from my.telegram.org)
    "api_id": int(os.environ["TELEGRAM_API_ID"]),
    "api_hash": os.environ["TELEGRAM_API_HASH"],
    "phone": os.environ["TELEGRAM_PHONE"],          # e.g. +251911234567
    "session_name": os.getenv("SESSION_NAME", "manager_session"),

    # Source channels to monitor (list of usernames or IDs)
    "source_channels": [
        c.strip()
        for c in os.environ["SOURCE_CHANNELS"].split(",")
    ],

    # Your destination channel (username or ID)
    "dest_channel": os.environ["DEST_CHANNEL"],

    # Channel niche / category for the AI filter
    "channel_category": os.getenv(
        "CHANNEL_CATEGORY",
        "Forex trading, gold (XAU/USD), commodities, crypto market analysis, economic news"
    ),

    # AI keys
    "gemini_api_key": os.environ["GEMINI_API_KEY"],
    "groq_api_key": os.environ["GROQ_API_KEY"],

    # Timing
    "poll_interval_seconds": int(os.getenv("POLL_INTERVAL", "60")),
    "min_delay_seconds": float(os.getenv("MIN_DELAY", "5")),
    "max_delay_seconds": float(os.getenv("MAX_DELAY", "25")),

    # Memory / dedup
    "db_path": os.getenv("DB_PATH", "memory.db"),
    "hash_ttl_days": int(os.getenv("HASH_TTL_DAYS", "30")),

    # Lookback window when first starting (hours)
    "lookback_hours": int(os.getenv("LOOKBACK_HOURS", "2")),
}


# ─── Graceful shutdown ─────────────────────────────────────────────────────────
_shutdown = asyncio.Event()


def _handle_signal(sig, frame):
    log.info(f"Signal {sig} received — initiating shutdown …")
    _shutdown.set()


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ─── Main loop ─────────────────────────────────────────────────────────────────
async def run():
    log.info("🚀  Telegram Channel Manager starting …")

    memory = MemoryManager(
        db_path=CONFIG["db_path"],
        ttl_days=CONFIG["hash_ttl_days"],
    )
    await memory.init()

    ai = AIEngine(
        gemini_key=CONFIG["gemini_api_key"],
        groq_key=CONFIG["groq_api_key"],
        channel_category=CONFIG["channel_category"],
    )

    scraper = ChannelScraper(
        config=CONFIG,
        ai_engine=ai,
        memory=memory,
    )

    await scraper.start()
    log.info("✅  Client connected. Entering poll loop …")

    try:
        while not _shutdown.is_set():
            try:
                await scraper.poll_and_forward()
            except Exception as exc:
                log.error(f"Poll cycle error: {exc}", exc_info=True)
            await asyncio.wait_for(
                _shutdown.wait(),
                timeout=CONFIG["poll_interval_seconds"],
            )
    except asyncio.TimeoutError:
        pass  # normal — loop again
    finally:
        log.info("🛑  Shutting down …")
        await scraper.stop()
        await memory.close()
        log.info("Goodbye.")


if __name__ == "__main__":
    asyncio.run(run())
