"""
ai_engine.py — Dual-layer AI analysis engine.

Two modes:
  • CALENDAR mode  — triggered when a message comes from CALENDAR_SOURCE.
                     Gemini scans for Red Folder (High Impact) events only.
                     Returns is_high_impact, event_name, news_time (HH:MM UTC).

  • NEWS mode      — standard Macro/Geopolitical analysis with Amharic hook.
                     Gemini primary → Groq fallback, same as before.

Style: Minimal rewrite. Keep source meaning almost identical.
       ONE Amharic sentence at top (hook only), rest is clean English.
"""

import asyncio
import base64
import json
import logging
import re
import textwrap
from datetime import datetime, timezone
from typing import Optional

import google.generativeai as genai
from groq import AsyncGroq

log = logging.getLogger("ai_engine")

# ══════════════════════════════════════════════════════════════════════════════
#  SYSTEM PROMPTS
# ══════════════════════════════════════════════════════════════════════════════

_NEWS_SYSTEM_PROMPT = """
You are AXIOM INTEL — a Macro & Geopolitical news editor for a Telegram channel.

YOUR ONLY JOB:
Take the source content, clean it up slightly, and post it in a structured \
format. Do NOT rewrite heavily. Do NOT add analysis. Do NOT change the meaning. \
Just format it professionally.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚫  REJECT IF ANY OF THESE APPLY:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. SIGNALS — Buy / Sell / Long / Short / Entry / TP / SL / price targets → REJECT
2. CHART ONLY — Image with no news context → REJECT
3. WATERMARK — Another channel's logo or username on image → REJECT
4. STALE — Content older than 18 hours → REJECT
5. OFF-TOPIC — Not about geopolitics, central banks, CPI/NFP/GDP, Gold, Oil → REJECT
6. LOW VALUE — Vague, no specific event or data → REJECT

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅  FORMAT (if approved):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[EMOJI] *[ONE short Amharic sentence — the headline only]*

[Source content lightly cleaned. English only. Bold instrument names and \
key numbers. 2-4 sentences max. Do not add anything new.]

⚠️ [One short English caution or context line — only if relevant.]

#PAIR1 #PAIR2

RULES:
• Amharic is used ONLY for the first hook sentence. Everything else is English.
• Do NOT mix Amharic words into the English body sentences.
• *bold* only: instrument names (*XAUUSD*, *DXY*) and key numbers (*5.25%*)
• Total post: 50-100 words MAX
• Hashtags only for instruments directly mentioned in the news
• No #forex #trading #market #signals

EMOJI — pick ONE:
  🚨 Breaking news     🌍 Geopolitics
  📊 Economic data     🏦 Central bank
  🛢️ Oil/energy        🏆 Gold

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPOND WITH JSON ONLY — NO MARKDOWN:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{
  "approved": true | false,
  "reason": "brief reason",
  "issues": [],
  "formatted_text": "post here (empty if rejected)",
  "hashtags": "#PAIR1 #PAIR2 (empty if rejected)",
  "confidence": 0.0
}
""".strip()


_CALENDAR_SYSTEM_PROMPT = """
You are AXIOM INTEL — Economic Calendar Vision Scanner.

YOUR ONLY JOB:
Scan the provided ForexFactory / economic calendar screenshot for HIGH IMPACT events.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔴  HIGH IMPACT = RED folder/bullet only
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• RED folder icon → High Impact → EXTRACT
• ORANGE folder  → Medium Impact → IGNORE
• YELLOW folder  → Low Impact   → IGNORE
• GREY folder    → Holiday/Info  → IGNORE

EXTRACTION RULES:
1. Look carefully at the color of each folder/bullet icon in the image.
2. For each RED icon found, extract:
   - event_name: the exact event label (e.g. "Non-Farm Payrolls", "CPI m/m", "FOMC Statement")
   - news_time:  the scheduled time shown, converted to HH:MM UTC format (24-hour)
3. If the image shows a different timezone (e.g. EST, GMT+3), convert it to UTC.
4. If NO red folder icons exist in the image → is_high_impact: false
5. If MULTIPLE red events exist, return only the EARLIEST upcoming one.
6. Do NOT invent events. Only extract what is visually present.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPOND WITH JSON ONLY — NO MARKDOWN:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{
  "is_high_impact": true | false,
  "event_name": "Event name or empty string",
  "news_time": "HH:MM UTC or empty string",
  "all_red_events": [
    {"event_name": "...", "news_time": "HH:MM UTC"}
  ],
  "reason": "brief explanation of what you saw"
}
""".strip()


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


_SIGNAL_RE = re.compile(
    r"\b(buy|sell|long|short|entry|tp|take[\s_-]?profit|sl|stop[\s_-]?loss|"
    r"stoploss|stop\s+at\s+\d|entry\s*[:\-]?\s*\d|target\s*[:\-]?\s*\d)\b",
    re.IGNORECASE,
)


def _signal_hit(text: str) -> Optional[str]:
    if not text:
        return None
    m = _SIGNAL_RE.search(text)
    return m.group(0).strip() if m else None


def _parse_news_json(raw: str) -> dict:
    raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            data = json.loads(m.group())
        else:
            raise ValueError(f"No JSON found:\n{raw[:200]}")

    data.setdefault("approved", False)
    data.setdefault("reason", "")
    data.setdefault("issues", [])
    data.setdefault("formatted_text", "")
    data.setdefault("hashtags", "")
    data.setdefault("confidence", 0.5)

    # Safety: reject if signal snuck into output
    if data.get("approved") and _signal_hit(data.get("formatted_text", "")):
        log.warning("Signal in output — hard reject.")
        data["approved"] = False
        data["reason"] = "Signal keyword found in output."
        data["issues"].append("signal_content")
        data["formatted_text"] = ""
        data["hashtags"] = ""

    return data


def _parse_calendar_json(raw: str) -> dict:
    raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            data = json.loads(m.group())
        else:
            raise ValueError(f"No calendar JSON found:\n{raw[:200]}")

    data.setdefault("is_high_impact", False)
    data.setdefault("event_name", "")
    data.setdefault("news_time", "")
    data.setdefault("all_red_events", [])
    data.setdefault("reason", "")

    # Validate time format
    if data.get("news_time"):
        if not re.match(r"^\d{2}:\d{2}(\s*UTC)?$", data["news_time"].strip()):
            log.warning(f"Suspicious time format: {data['news_time']} — clearing.")
            data["news_time"] = ""
            data["is_high_impact"] = False

    return data


# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE
# ══════════════════════════════════════════════════════════════════════════════

class AIEngine:
    def __init__(self, gemini_key: str, groq_key: str, channel_category: str):
        self._category = channel_category
        self._groq = AsyncGroq(api_key=groq_key)

        genai.configure(api_key=gemini_key)

        # News model — standard analysis
        self._gemini_news = genai.GenerativeModel(
            model_name="gemini-2.5-flash-preview-05-20",
            system_instruction=_NEWS_SYSTEM_PROMPT,
            generation_config=genai.GenerationConfig(
                temperature=0.15,
                max_output_tokens=350,
                response_mime_type="application/json",
            ),
        )

        # Calendar model — vision scanner (slightly higher temp for visual reasoning)
        self._gemini_calendar = genai.GenerativeModel(
            model_name="gemini-2.5-flash-preview-05-20",
            system_instruction=_CALENDAR_SYSTEM_PROMPT,
            generation_config=genai.GenerationConfig(
                temperature=0.1,
                max_output_tokens=400,
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
        """Standard news analysis (Gemini → Groq fallback)."""

        # Pre-filter signals
        hit = _signal_hit(text)
        if hit:
            log.info(f"[PRE-FILTER] Signal '{hit}' — instant reject.")
            return {
                "approved": False,
                "reason": f"Signal keyword: '{hit}'",
                "issues": ["signal_content"],
                "formatted_text": "",
                "hashtags": "",
                "confidence": 1.0,
                "engine": "pre_filter",
            }

        prompt = self._build_news_prompt(text)

        # Layer 1 — Gemini
        try:
            verdict = await asyncio.wait_for(
                self._gemini_news_call(prompt, image_data, image_mime), timeout=35
            )
            verdict["engine"] = "gemini-2.5-flash"
            log.info(f"Gemini NEWS → approved={verdict['approved']} | {verdict['reason']}")
            return verdict
        except Exception as exc:
            log.warning(f"Gemini NEWS failed ({exc}) — trying Groq …")

        # Layer 2 — Groq fallback
        try:
            verdict = await asyncio.wait_for(
                self._groq_news_call(prompt, image_data, image_mime), timeout=50
            )
            verdict["engine"] = "groq-llama4-vision"
            log.info(f"Groq NEWS → approved={verdict['approved']} | {verdict['reason']}")
            return verdict
        except Exception as exc:
            log.error(f"Groq NEWS failed ({exc}) — safe reject.")
            return {
                "approved": False,
                "reason": "Both engines unavailable.",
                "issues": ["engine_error"],
                "formatted_text": "",
                "hashtags": "",
                "confidence": 0.0,
                "engine": "none",
            }

    async def analyse_calendar(
        self,
        image_data: bytes,
        image_mime: str = "image/jpeg",
        text: str = "",
    ) -> dict:
        """
        Calendar vision analysis — scans for Red Folder (High Impact) events.
        Gemini only (vision required). Returns calendar-specific JSON.
        """
        if not image_data:
            log.warning("Calendar analysis called without image — skipping.")
            return {
                "is_high_impact": False,
                "event_name": "",
                "news_time": "",
                "all_red_events": [],
                "reason": "No image provided.",
                "engine": "none",
            }

        prompt = self._build_calendar_prompt(text)

        try:
            result = await asyncio.wait_for(
                self._gemini_calendar_call(prompt, image_data, image_mime), timeout=40
            )
            result["engine"] = "gemini-2.5-flash-calendar"
            log.info(
                f"Calendar scan → high_impact={result['is_high_impact']} | "
                f"event='{result.get('event_name')}' | "
                f"time='{result.get('news_time')}' | "
                f"reason='{result.get('reason')}'"
            )
            return result
        except Exception as exc:
            log.error(f"Calendar Gemini failed ({exc}) — safe reject.")
            return {
                "is_high_impact": False,
                "event_name": "",
                "news_time": "",
                "all_red_events": [],
                "reason": f"Engine error: {exc}",
                "engine": "none",
            }

    # ── Prompt builders ────────────────────────────────────────────────────────

    def _build_news_prompt(self, text: str) -> str:
        return textwrap.dedent(f"""
            DATE (UTC): {_today_str()}
            CHANNEL FOCUS: {self._category}

            SOURCE:
            \"\"\"
            {text.strip() if text else "(image only)"}
            \"\"\"

            TASK:
            1. Check all rejection criteria first.
            2. If image: scan for watermarks, check timestamps.
            3. If approved: lightly clean and format. Keep it close to source.
               ONE Amharic hook sentence only. Rest is clean English. 50-100 words max.
            4. Return JSON only.
        """).strip()

    def _build_calendar_prompt(self, text: str = "") -> str:
        return textwrap.dedent(f"""
            DATE (UTC): {_today_str()}

            Carefully examine the economic calendar screenshot.
            Look at the COLOUR of each folder/impact icon:
              🔴 RED   = High Impact → EXTRACT
              🟠 ORANGE = Medium    → IGNORE
              🟡 YELLOW = Low       → IGNORE

            {f'Caption text from source: "{text.strip()}"' if text.strip() else ""}

            Return JSON only — no markdown.
        """).strip()

    # ── Gemini calls ───────────────────────────────────────────────────────────

    async def _gemini_news_call(self, prompt, image_data, image_mime):
        parts = []
        if image_data:
            parts.append({"inline_data": {"mime_type": image_mime, "data": _b64(image_data)}})
        parts.append(prompt)
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(
            None, lambda: self._gemini_news.generate_content(parts)
        )
        return _parse_news_json(resp.text)

    async def _gemini_calendar_call(self, prompt, image_data, image_mime):
        parts = [
            {"inline_data": {"mime_type": image_mime, "data": _b64(image_data)}},
            prompt,
        ]
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(
            None, lambda: self._gemini_calendar.generate_content(parts)
        )
        return _parse_calendar_json(resp.text)

    # ── Groq fallback (news only) ──────────────────────────────────────────────

    async def _groq_news_call(self, prompt, image_data, image_mime):
        content = []
        if image_data:
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:{image_mime};base64,{_b64(image_data)}"},
            })
        content.append({"type": "text", "text": prompt})
        resp = await self._groq.chat.completions.create(
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            messages=[
                {"role": "system", "content": _NEWS_SYSTEM_PROMPT},
                {"role": "user",   "content": content},
            ],
            temperature=0.15,
            max_tokens=350,
        )
        return _parse_news_json(resp.choices[0].message.content)
