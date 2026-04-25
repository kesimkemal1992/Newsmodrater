"""
generate_session.py — Run this ONCE locally to get your STRING SESSION.

After running, copy the printed string and paste it into Railway as:
    SESSION_STRING = 1BVtsOK...

Usage:
    pip install telethon python-dotenv
    python generate_session.py
"""

import os
from dotenv import load_dotenv

load_dotenv()

def main():
    try:
        from telethon.sync import TelegramClient
        from telethon.sessions import StringSession
    except ImportError:
        print("❌  Run: pip install telethon")
        return

    print("=" * 55)
    print("  Telegram String Session Generator")
    print("=" * 55)
    print()

    api_id_raw = os.getenv("TELEGRAM_API_ID") or input("Enter API_ID (from my.telegram.org): ").strip()
    api_hash   = os.getenv("TELEGRAM_API_HASH") or input("Enter API_HASH: ").strip()
    phone      = os.getenv("TELEGRAM_PHONE") or input("Enter phone (e.g. +251911234567): ").strip()

    try:
        api_id = int(api_id_raw)
    except ValueError:
        print("❌  API_ID must be a number.")
        return

    print(f"\n📲  Connecting and sending OTP to {phone} …\n")

    with TelegramClient(StringSession(), api_id, api_hash) as client:
        client.start(phone=phone)
        session_string = client.session.save()

    print("\n" + "=" * 55)
    print("✅  SUCCESS! Your string session:")
    print("=" * 55)
    print()
    print(session_string)
    print()
    print("=" * 55)
    print("👉  Copy the string above and add it to Railway as:")
    print("    Variable name : SESSION_STRING")
    print("    Variable value: (the long string above)")
    print()
    print("⚠️  Keep this string SECRET — it's like a password.")
    print("=" * 55)


if __name__ == "__main__":
    main()
