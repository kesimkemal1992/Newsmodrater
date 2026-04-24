"""
ai_engine.py — Dual-layer AI analysis engine.

Layer 1 (Primary)  : Google Gemini 2.5 Flash — text + vision
Layer 2 (Fallback) : Groq (llama-3.2-11b-vision-preview) — text + vision

Strict Geopolitical & Macro Intelligence Analyst:
  • REJECT any signal containing Buy/Sell/Entry/TP/SL or price levels
  • ONLY approve: global conflicts, elections, central bank decisions,
    CPI/inflation, oil/gold moves driven by fundamental news
  • REJECT technical-only charts (no fundamental/geopolitical explanation)
  • Detect watermarks/logos → REJECT
  • Validate dates – content older than 18 hours → REJECT
  • Format: Amharic‑English mix, emojis, only affected pair hashtags
"""

import asyncio
import base64
import json
import logging
import re
import textwrap
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
import google.generativeai as genai
from groq import AsyncGroq

log = logging.getLogger("ai_engine")

# ─── Verdict schema ────────────────────────────────────────────────────────────
# {
#   "approved": bool,
#   "reason": str,
#   "formatted_text": str,       # only if approved
#   "hashtags": str,             # e.g. "#XAUUSD #USD #GOLD"
#   "confidence": float,         # 0.0–1.0
#   "issues": list[str],         # e.g. ["watermark", "price_signal", "old_content"]
# }

_SYSTEM_PROMPT = """
You are AXIOM – a Geopolitical & Macro Intelligence Analyst for a premium financial Telegram channel.

**CRITICAL – YOU ARE NOT A TRADING SIGNAL BOT.**  
Reject any content that resembles a trade recommendation.

### STRICT REJECTION RULES (if ANY apply → approved=false):

1. **PRICE SIGNALS** – Contains any of: Buy, Sell, Entry, TP, SL, Take Profit, Stop Loss, specific price levels (e.g., "1.2050 entry", "target 1850"). REJECT immediately.

2. **TECHNICAL-ONLY** – Plain chart patterns, RSI, MACD, support/resistance without a clear fundamental or geopolitical news explanation. REJECT.

3. **OFF-TOPIC (Non-Macro/Geopolitical)** – Content not related to:
   - Global conflicts (wars, sanctions, diplomatic ruptures)
   - Major elections (US, EU, UK, Japan, etc.)
   - Central bank decisions (FED, ECB, BOJ, BOE, PBoC)
   - Inflation data (CPI, PPI, PCE)
   - Oil or Gold moves driven by fundamental news (supply shocks, OPEC+, central bank purchases)
   Memes, celebrity news, pure crypto hype → REJECT.

4. **WATERMARK / LOGO** – Image contains another channel's watermark, logo, username, or brand. REJECT.

5. **STALE CONTENT** – Any visible date on chart, news clip, or screenshot older than 18 hours from now (current UTC). If uncertain but looks old → REJECT.

6. **DUPLICATE / LOW-VALUE** – Generic "market moving" without specific event, source, or insight. REJECT.

7. **UNVERIFIABLE CLAIMS** – Wild predictions without data source. REJECT.

### IF APPROVED – FORMATTING (Professional Urban Amharic‑English mix):
- **Hook** (Amharic) – compelling 1-liner with emoji: 🚨🌍📊⚠️
- **Body** – English with Amharic phrases woven naturally. Explain the event and its market implication.
- **No copy-paste** – rewrite with added analytical value.
- **Clean Telegram markup** – *bold* for key instruments, #hashtags for pairs only.
- **Footer** – Only hashtags of affected instruments: #GOLD #XAUUSD #USD #OIL #EURUSD (no generic #forex #trading).
- Length: 150–280 words. Concise institutional style.

### OUTPUT FORMAT – valid JSON only:
{
  "approved": true/false,
  "reason": "brief reason",
  "issues": ["price_signal", "off_topic", "watermark", "old_content", ...],
  "formatted_text": "... (only if approved)",
  "hashtags": "#PAIR1 #PAIR2 (only if approved)",
  "confidence": 0.0-1.0
}
"""


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _is_older_than_18h(dt: datetime) -> bool:
    """Compare given datetime (naive assumed UTC) with current UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return _now_utc() - dt > timedelta(hours=18)


class AIEngine:
    def __init__(self, gemini_key: str, groq_key: str, channel_category: str):
        self._category = channel_category
        self._groq = AsyncGroq(api_key=groq_key)

        # Configure Gemini
        genai.configure(api_key=gemini_key)
        self._gemini = genai.GenerativeModel(
            model_name="gemini-2.5-flash-preview-05-20",
            system_instruction=_SYSTEM_PROMPT,
            generation_config=genai.GenerationConfig(
                temperature=0.2,          # lower for strict rule following
                max_output_tokens=1024,
                response_mime_type="application/json",
            ),
        )

    # ── Public API ─────────────────────────────────────────────────────────────
    async def analyse(
        self,
        text: str,
        image_data: Optional[bytes] = None,
        image_mime: str = "image/jpeg",
    ) -> dict:
        """Returns verdict dict. Tries Gemini → Groq fallback."""
        user_prompt = self._build_user_prompt(text)

        # --- Layer 1: Gemini 2.5 Flash ---
        try:
            verdict = await asyncio.wait_for(
                self._gemini_analyse(user_prompt, image_data, image_mime),
                timeout=30,
            )
            verdict["engine"] = "gemini"
            log.info(f"Gemini: approved={verdict['approved']} | {verdict['reason']}")
            # Post-process date validation for safety
            self._validate_date_manually(verdict, text, image_data, image_mime)
            return verdict
        except Exception as exc:
            log.warning(f"Gemini failed ({exc}), falling back to Groq…")

        # --- Layer 2: Groq (vision-capable) ---
        try:
            verdict = await asyncio.wait_for(
                self._groq_analyse(user_prompt, image_data, image_mime),
                timeout=40,
            )
            verdict["engine"] = "groq"
            log.info(f"Groq: approved={verdict['approved']} | {verdict['reason']}")
            self._validate_date_manually(verdict, text, image_data, image_mime)
            return verdict
        except Exception as exc:
            log.error(f"Both AI engines failed: {exc}")
            return {
                "approved": False,
                "reason": "Both AI engines failed – skipping for safety.",
                "issues": ["engine_error"],
                "formatted_text": "",
                "hashtags": "",
                "confidence": 0.0,
                "engine": "none",
            }

    # ── Prompt builder ─────────────────────────────────────────────────────────
    def _build_user_prompt(self, text: str) -> str:
        now_str = _now_utc().strftime("%Y-%m-%d %H:%M:%S UTC")
        return textwrap.dedent(f"""
            CURRENT TIME (UTC): {now_str}
            CHANNEL CATEGORY: {self._category}

            SOURCE TEXT:
            \"\"\"
            {text or "(no text – image only)"}
            \"\"\"

            INSTRUCTIONS:
            1. If image is attached, scan for watermarks/logos/dates.
            2. Apply ALL rejection rules. Check for price signals (Buy/Sell/Entry/TP/SL/price levels).
            3. If approved, produce formatted post (Amharic-English mix, emojis, hashtags only for affected instruments).
            4. Return ONLY the JSON object.
        """).strip()

    # ── Gemini call ────────────────────────────────────────────────────────────
    async def _gemini_analyse(
        self,
        prompt: str,
        image_data: Optional[bytes],
        image_mime: str,
    ) -> dict:
        parts = []
        if image_data:
            parts.append({"inline_data": {"mime_type": image_mime, "data": _b64(image_data)}})
        parts.append(prompt)

        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: self._gemini.generate_content(parts),
        )
        return self._parse_json(response.text)

    # ── Groq call (vision model) ───────────────────────────────────────────────
    async def _groq_analyse(
        self,
        prompt: str,
        image_data: Optional[bytes],
        image_mime: str,
    ) -> dict:
        messages_content = []
        if image_data:
            messages_content.append({
                "type": "image_url",
                "image_url": {"url": f"data:{image_mime};base64,{_b64(image_data)}"},
            })
        messages_content.append({"type": "text", "text": prompt})

        response = await self._groq.chat.completions.create(
            model="llama-3.2-11b-vision-preview",   # vision-capable fallback
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": messages_content},
            ],
            temperature=0.2,
            max_tokens=1024,
        )
        raw = response.choices[0].message.content
        return self._parse_json(raw)

    # ── JSON parser ───────────────────────────────────────────────────────────
    @staticmethod
    def _parse_json(raw: str) -> dict:
        raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            if m:
                data = json.loads(m.group())
            else:
                raise ValueError(f"No JSON found: {raw[:200]}")
        data.setdefault("approved", False)
        data.setdefault("reason", "")
        data.setdefault("issues", [])
        data.setdefault("formatted_text", "")
        data.setdefault("hashtags", "")
        data.setdefault("confidence", 0.5)
        return data

    # ── Manual date overrule (safety net) ──────────────────────────────────────
    def _validate_date_manually(self, verdict: dict, text: str,
                                image_data: Optional[bytes], image_mime: str):
        """If AI missed an obvious old date, force rejection."""
        if not verdict.get("approved", False):
            return

        # Simple regex to find dates like "2025-01-15", "Jan 15, 2025", "15/01/2025"
        date_patterns = [
            r"\d{4}-\d{1,2}-\d{1,2}",           # 2025-01-15
            r"\d{1,2}/\d{1,2}/\d{4}",            # 15/01/2025
            r"[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}", # Jan 15, 2025
        ]
        found_dates = []
        for pat in date_patterns:
            matches = re.findall(pat, text)
            found_dates.extend(matches)

        # If any parsed date is > 18h old, reject
        for date_str in found_dates:
            try:
                # Try common formats
                if "-" in date_str:
                    dt = datetime.strptime(date_str.split()[0], "%Y-%m-%d")
                elif "/" in date_str:
                    dt = datetime.strptime(date_str, "%d/%m/%Y")
                else:
                    dt = datetime.strptime(date_str, "%B %d, %Y")
                dt = dt.replace(tzinfo=timezone.utc)
                if _is_older_than_18h(dt):
                    verdict["approved"] = False
                    verdict["reason"] = f"Found stale date: {date_str} (>18h old)"
                    verdict["issues"].append("old_content")
                    verdict["formatted_text"] = ""
                    verdict["hashtags"] = ""
                    log.warning(f"Manual date rejection: {date_str}")
                    return
            except Exception:
                continue
