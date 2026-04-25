"""
login.py — One-time Telegram user account login helper.

Run this ONCE locally to generate your .session file before deploying
to Railway or Render. The session file allows the bot to connect without
re-entering your phone/OTP on the server.

Usage:
    python login.py
"""

import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


async def main():
    try:
        from telethon import TelegramClient
        from telethon.errors import SessionPasswordNeededError
    except ImportError:
        print("❌  Telethon not installed. Run: pip install -r requirements.txt")
        sys.exit(1)

    print("=" * 55)
    print("  Telegram User Account — Session Generator")
    print("=" * 55)
    print()

    # ── Read credentials ───────────────────────────────────────
    api_id_raw = os.getenv("TELEGRAM_API_ID") or input("Enter API_ID (from my.telegram.org): ").strip()
    api_hash = os.getenv("TELEGRAM_API_HASH") or input("Enter API_HASH: ").strip()
    phone = os.getenv("TELEGRAM_PHONE") or input("Enter phone number (e.g. +251911234567): ").strip()
    session_name = os.getenv("SESSION_NAME", "manager_session")

    try:
        api_id = int(api_id_raw)
    except ValueError:
        print("❌  API_ID must be a number.")
        sys.exit(1)

    print(f"\n📡  Connecting to Telegram …")
    print(f"    Session file will be: {session_name}.session")
    print()

    client = TelegramClient(session_name, api_id, api_hash)

    await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"✅  Already logged in as: {me.first_name} (@{me.username or me.id})")
        print(f"\n✅  Session file ready: {session_name}.session")
        await client.disconnect()
        return

    # ── Send OTP ───────────────────────────────────────────────
    print(f"📲  Sending OTP code to {phone} …")
    await client.send_code_request(phone)

    otp = input("Enter the OTP code you received: ").strip().replace(" ", "")

    try:
        await client.sign_in(phone, otp)

    except SessionPasswordNeededError:
        # Two-step verification enabled
        print("\n🔐  Two-step verification is enabled on this account.")
        password = input("Enter your 2FA password: ").strip()
        await client.sign_in(password=password)

    except Exception as e:
        print(f"❌  Login failed: {e}")
        await client.disconnect()
        sys.exit(1)

    me = await client.get_me()
    print(f"\n✅  Logged in successfully as: {me.first_name} (@{me.username or me.id})")
    print(f"✅  Session saved to: {session_name}.session")
    print()
    print("─" * 55)
    print("NEXT STEPS:")
    print(f"  1. Upload '{session_name}.session' to your server's")
    print(f"     persistent volume (Railway: /app/data/, Render: /var/data/)")
    print(f"  2. Set SESSION_NAME env var to match (without .session)")
    print(f"  3. Deploy and run: python main.py")
    print("─" * 55)

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
