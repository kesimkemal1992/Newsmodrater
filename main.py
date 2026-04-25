"""
main.py — AXIOM INTEL Telegram Manager
Uses StringSession so no interactive OTP is needed on Railway/Render.

First time setup:
    python generate_session.py   ← run locally, get SESSION_STRING
    Paste SESSION_STRING into Railway environment variables.
    Deploy. Done.
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


# ─── Config ────────────────────────────────────────────────────────────────────
def _require(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        log.error(f"❌  Missing required env var: {key}")
        sys.exit(1)
    return val


CONFIG = {
    # Telegram credentials
    "api_id":         int(_require("TELEGRAM_API_ID")),
    "api_hash":       _require("TELEGRAM_API_HASH"),
    "phone":          os.getenv("TELEGRAM_PHONE", ""),   # optional when using string session

    # ✅ String session — no OTP needed on server
    "session_string": os.getenv("SESSION_STRING", ""),   # preferred
    "session_name":   os.getenv("SESSION_NAME", "manager_session"),  # fallback (local file)

    # Channels
    "source_channels": [
        c.strip() for c in _require("SOURCE_CHANNELS").split(",") if c.strip()
    ],
    "dest_channel": _require("DEST_CHANNEL"),

    # AI keys
    "gemini_api_key": _require("GEMINI_API_KEY"),
    "groq_api_key":   _require("GROQ_API_KEY"),

    # Channel focus (injected into every AI prompt)
    "channel_category": os.getenv(
        "CHANNEL_CATEGORY",
        "Geopolitical events (wars, sanctions, elections), Central Bank policy "
        "(FED, ECB, BOE, BOJ), Macroeconomic data (CPI, NFP, GDP), "
        "Gold (XAU) safe-haven flows, Oil (WTI/Brent) supply disruptions. "
        "NO trading signals. NO technical-only charts.",
    ),

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
    log.info(f"Signal {sig} received — shutting down …")
    _shutdown.set()

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ─── Main ──────────────────────────────────────────────────────────────────────
async def run():
    log.info("🚀  AXIOM INTEL — Geopolitical Channel Manager starting …")
    log.info(f"📡  Monitoring {len(CONFIG['source_channels'])} source channel(s)")
    log.info(f"📤  Destination: {CONFIG['dest_channel']}")

    if CONFIG["session_string"]:
        log.info("🔑  Auth mode: StringSession ✅")
    else:
        log.info("🔑  Auth mode: File session (ensure .session file exists)")

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
    log.info("✅  Telegram client connected. Entering poll loop …")

    try:
        while not _shutdown.is_set():
            try:
                await scraper.poll_and_forward()
            except Exception as exc:
                log.error(f"Poll cycle error: {exc}", exc_info=True)

            try:
                await asyncio.wait_for(
                    _shutdown.wait(),
                    timeout=CONFIG["poll_interval_seconds"],
                )
            except asyncio.TimeoutError:
                pass  # normal — loop again
    finally:
        log.info("🛑  Shutting down gracefully …")
        await scraper.stop()
        await memory.close()
        log.info("👋  Goodbye.")


if __name__ == "__main__":
    asyncio.run(run())
