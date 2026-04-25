"""
ai_engine.py — Dual-layer AI analysis engine.

Primary  : Gemini 2.5 Flash  (google-generativeai)
Fallback : Groq llama-4-scout (vision capable)

Style: Senior Institutional Trader — English only, minimalist, direct.
       No personal opinions. Data-first. 12-hour AM/PM time format.
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

# ─── Institutional Moderation Prompt ──────────────────────────────────────────
_SYSTEM_PROMPT = """
You are AXIOM INTEL — a Senior Institutional Macro & Geopolitical news editor
for a professional Telegram trading channel. Your audience is experienced traders.

YOUR ONLY JOB:
Take the source content, verify its relevance, and format it in a clean
institutional style. Do NOT speculate. Do NOT add analysis beyond the facts.
Do NOT change the meaning. Format it professionally and precisely.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚫  REJECT IF ANY OF THESE APPLY:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. SIGNALS       — Buy / Sell / Long / Short / Entry / TP / SL / price targets
2. CHART ONLY    — Image with no news context
3. WATERMARK     — Another channel's logo or username on image
4. STALE         — Content older than 18 hours
5. OFF-TOPIC     — Not about geopolitics, central banks, CPI/NFP/GDP,
                   Gold, Oil, major FX pairs, interest rates
6. LOW VALUE     — Vague, no specific event or data point
7. DUPLICATE     — Essentially same content already processed

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅  FORMAT (if approved):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[EMOJI] *[SHORT ENGLISH HEADLINE — one line, factual and direct]*

[Source content lightly cleaned. English only. Bold instrument names and
key numbers. 2-4 sentences max. Do not add anything new.]

📌 MARKET STATUS: [ONE short line on likely market impact — e.g.,
   "Focus: USD liquidity expansion ahead of FOMC." or
   "Gold safe-haven demand elevated on geopolitical risk."]

#PAIR1 #PAIR2

RULES:
• English ONLY. No Amharic or other languages.
• *bold* only for: instrument names (*XAUUSD*, *DXY*) and key numbers (*5.25%*)
• 12-hour time format: 03:30 PM, 08:00 AM (never 24-hour)
• Total post: 60-120 words MAX
• Hashtags only for instruments directly mentioned
• No #forex #trading #market #signals #news

EMOJI — pick ONE that matches:
  🚨 Breaking news     🌍 Geopolitics
  📊 Economic data     🏦 Central bank
  🛢️ Oil/energy        🏆 Gold/commodities
  💵 USD/FX flows      ⚠️ Risk event

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPOND WITH VALID JSON ONLY — NO MARKDOWN FENCES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{
  "approved": true | false,
  "reason": "brief reason",
  "issues": [],
  "formatted_text": "full post here (empty if rejected)",
  "hashtags": "#PAIR1 #PAIR2 (empty if rejected)",
  "confidence": 0.0
}
""".strip()

# ─── Forex Factory Daily Briefing Prompt ──────────────────────────────────────
_BRIEFING_PROMPT_TEMPLATE = """
You are AXIOM INTEL — a Senior Institutional Macro trader.

Today's high-impact forex factory events (Red + Orange impact only) are listed below.
Format them into a professional DAILY NEWS BRIEFING.

EVENTS (JSON):
{events_json}

OUTPUT FORMAT (Telegram Markdown, strictly follow this):
```
📅 *TODAY'S HIGH IMPACT NEWS*
{date_str}

{for each event sorted by time:}
🔴 {time_12h} | {currency}: {event_name}
• Forecast: {forecast} | Previous: {previous}

{if orange impact use 🟠 instead of 🔴}
```

After listing all events, add ONE line:
📌 *MARKET STATUS:* [Brief institutional assessment — e.g.,
"USD under pressure ahead of NFP release. Gold may see volatility."]

RULES:
- English only. Institutional tone. No opinions.
- Times in 12-hour AM/PM format (Africa/Addis_Ababa GMT+3).
- If Forecast or Previous is unknown, write "—"
- Sort events chronologically
- Only include Red (🔴) and Orange (🟠) impact events
- Do NOT add any signals, advice, or trade recommendations

Return ONLY the formatted message text. No JSON. No explanations.
""".strip()

# ─── 10-Minute Alert Prompt ────────────────────────────────────────────────────
_ALERT_PROMPT_TEMPLATE = """
You are AXIOM INTEL — Senior Institutional Trader.

Generate a professional 10-MINUTE WARNING alert for this upcoming event:

EVENT NAME: {event_name}
CURRENCY: {currency}
TIME: {time_12h}
FORECAST: {forecast}
PREVIOUS: {previous}
IMPACT: {impact}

OUTPUT FORMAT (Telegram Markdown, strictly follow):
```
🚨 *ALERT: 10 MINUTES REMAINING*

EVENT: {impact_emoji} {event_name}
TIME: {time_12h} EAT
FORECAST: {forecast}
PREVIOUS: {previous}

*REQUIRED ACTION:*
✅ Secure open profits
✅ Move Stop-Loss to Break-even
✅ No new entries during release

*PROTECT YOUR CAPITAL. NO GAMBLING.*
```

Return ONLY the formatted alert text. No JSON. No explanations.
""".strip()

# ─── Weekly Outlook Prompt ─────────────────────────────────────────────────────
_WEEKLY_PROMPT_TEMPLATE = """
You are AXIOM INTEL — Senior Institutional Trader.

Generate a professional WEEKLY OUTLOOK briefing for the coming week.
Events are listed below (Red + Orange only):

EVENTS (JSON):
{events_json}

Week range: {week_range}

OUTPUT FORMAT (Telegram Markdown):
```
📅 *WEEKLY HIGH IMPACT OUTLOOK*
Week of {week_range}

{Group events by day, sorted chronologically}
*{DAY NAME} — {Date}*
🔴 {time_12h} | {currency}: {event_name}
  ↳ Forecast: {forecast} | Previous: {previous}

📌 *KEY FOCUS THIS WEEK:*
[2-3 sentence institutional summary of the most critical events and
expected market themes — NFP, FOMC, CPI etc. English only. Factual.]

```

Return ONLY the formatted message. No JSON. No explanations.
""".strip()


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


def _parse_json(raw: str) -> dict:
    raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            data = json.loads(m.group())
        else:
            raise ValueError(f"No JSON found:\n{raw[:300]}")

    data.setdefault("approved", False)
    data.setdefault("reason", "")
    data.setdefault("issues", [])
    data.setdefault("formatted_text", "")
    data.setdefault("hashtags", "")
    data.setdefault("confidence", 0.5)

    # Safety: reject if signal snuck into output
    if data.get("approved") and _signal_hit(data.get("formatted_text", "")):
        log.warning("Signal keyword detected in output — hard reject.")
        data["approved"] = False
        data["reason"] = "Signal keyword found in formatted output."
        data["issues"].append("signal_content")
        data["formatted_text"] = ""
        data["hashtags"] = ""

    return data


class AIEngine:
    def __init__(self, gemini_key: str, groq_key: str, channel_category: str):
        self._category = channel_category
        self._groq = AsyncGroq(api_key=groq_key)

        genai.configure(api_key=gemini_key)

        # ✅ Fixed: use the correct stable model name
        self._gemini = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            system_instruction=_SYSTEM_PROMPT,
            generation_config=genai.GenerationConfig(
                temperature=0.15,
                max_output_tokens=500,
                response_mime_type="application/json",
            ),
        )

        # Gemini model without forced JSON mode — for free-text generation
        self._gemini_text = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config=genai.GenerationConfig(
                temperature=0.2,
                max_output_tokens=1200,
            ),
        )

    # ── News Moderation (existing flow) ───────────────────────────────────────
    async def analyse(
        self,
        text: str,
        image_data: Optional[bytes] = None,
        image_mime: str = "image/jpeg",
    ) -> dict:
        """Analyse a scraped Telegram message for approval/rejection."""

        # Pre-filter signals before paying for API call
        hit = _signal_hit(text)
        if hit:
            log.info(f"[PRE-FILTER] Signal keyword '{hit}' — instant reject.")
            return {
                "approved": False,
                "reason": f"Signal keyword detected: '{hit}'",
                "issues": ["signal_content"],
                "formatted_text": "",
                "hashtags": "",
                "confidence": 1.0,
                "engine": "pre_filter",
            }

        prompt = self._build_moderation_prompt(text)

        # Layer 1 — Gemini 2.5 Flash (Primary)
        try:
            verdict = await asyncio.wait_for(
                self._gemini_call(prompt, image_data, image_mime), timeout=40
            )
            verdict["engine"] = "gemini-2.5-flash"
            log.info(
                f"Gemini NEWS → approved={verdict['approved']} | {verdict.get('reason', '')}"
            )
            return verdict
        except Exception as exc:
            log.warning(f"Gemini NEWS failed ({exc}) — trying Groq …")

        # Layer 2 — Groq (Fallback)
        try:
            verdict = await asyncio.wait_for(
                self._groq_call(prompt, image_data, image_mime), timeout=55
            )
            verdict["engine"] = "groq-llama4-vision"
            log.info(
                f"Groq NEWS → approved={verdict['approved']} | {verdict.get('reason', '')}"
            )
            return verdict
        except Exception as exc:
            log.error(f"Groq NEWS failed ({exc}) — safe reject.")
            return {
                "approved": False,
                "reason": "Both AI engines unavailable.",
                "issues": ["engine_error"],
                "formatted_text": "",
                "hashtags": "",
                "confidence": 0.0,
                "engine": "none",
            }

    # ── Daily Briefing Generation ──────────────────────────────────────────────
    async def generate_daily_briefing(self, events: list, date_str: str) -> str:
        """Generate today's high-impact forex news briefing from events list."""
        if not events:
            return ""

        prompt = _BRIEFING_PROMPT_TEMPLATE.format(
            events_json=json.dumps(events, ensure_ascii=False, indent=2),
            date_str=date_str,
        )

        try:
            result = await asyncio.wait_for(
                self._gemini_text_call(prompt), timeout=45
            )
            log.info("Daily briefing generated via Gemini.")
            return result.strip()
        except Exception as exc:
            log.warning(f"Gemini briefing failed ({exc}) — trying Groq …")

        try:
            result = await asyncio.wait_for(
                self._groq_text_call(prompt), timeout=60
            )
            log.info("Daily briefing generated via Groq fallback.")
            return result.strip()
        except Exception as exc:
            log.error(f"Groq briefing failed ({exc}) — using fallback format.")
            return self._fallback_briefing(events, date_str)

    # ── 10-Minute Alert Generation ─────────────────────────────────────────────
    async def generate_alert(self, event: dict) -> str:
        """Generate a 10-minute pre-event warning alert."""
        impact_emoji = "🔴" if event.get("impact") == "red" else "🟠"
        prompt = _ALERT_PROMPT_TEMPLATE.format(
            event_name=event.get("name", "Unknown Event"),
            currency=event.get("currency", "USD"),
            time_12h=event.get("time_12h", "—"),
            forecast=event.get("forecast", "—"),
            previous=event.get("previous", "—"),
            impact=event.get("impact", "red"),
            impact_emoji=impact_emoji,
        )

        try:
            result = await asyncio.wait_for(
                self._gemini_text_call(prompt), timeout=30
            )
            log.info(f"Alert generated for: {event.get('name')}")
            return result.strip()
        except Exception as exc:
            log.warning(f"Gemini alert failed ({exc}) — using fallback.")

        try:
            result = await asyncio.wait_for(
                self._groq_text_call(prompt), timeout=45
            )
            return result.strip()
        except Exception as exc:
            log.error(f"Groq alert failed ({exc}) — using fallback format.")
            return self._fallback_alert(event)

    # ── Weekly Outlook Generation ──────────────────────────────────────────────
    async def generate_weekly_outlook(self, events: list, week_range: str) -> str:
        """Generate Sunday weekly outlook from scraped events."""
        if not events:
            return ""

        prompt = _WEEKLY_PROMPT_TEMPLATE.format(
            events_json=json.dumps(events, ensure_ascii=False, indent=2),
            week_range=week_range,
        )

        try:
            result = await asyncio.wait_for(
                self._gemini_text_call(prompt), timeout=60
            )
            log.info("Weekly outlook generated via Gemini.")
            return result.strip()
        except Exception as exc:
            log.warning(f"Gemini weekly failed ({exc}) — trying Groq …")

        try:
            result = await asyncio.wait_for(
                self._groq_text_call(prompt), timeout=75
            )
            log.info("Weekly outlook generated via Groq fallback.")
            return result.strip()
        except Exception as exc:
            log.error(f"Weekly outlook generation failed: {exc}")
            return self._fallback_weekly(events, week_range)

    # ── Internal API Callers ───────────────────────────────────────────────────
    def _build_moderation_prompt(self, text: str) -> str:
        return textwrap.dedent(f"""
            DATE (UTC): {_today_str()}
            CHANNEL FOCUS: {self._category}

            SOURCE CONTENT:
            \"\"\"
            {text.strip() if text else "(image only — no text)"}
            \"\"\"

            TASK:
            1. Check ALL rejection criteria first.
            2. If image present: scan for watermarks or signals, check timestamps.
            3. If approved: lightly clean and format. English only. 60-120 words max.
               Add a MARKET STATUS line. Use 12-hour AM/PM times.
            4. Return valid JSON only — no markdown fences.
        """).strip()

    async def _gemini_call(self, prompt: str, image_data: Optional[bytes], image_mime: str) -> dict:
        parts = []
        if image_data:
            parts.append({"inline_data": {"mime_type": image_mime, "data": _b64(image_data)}})
        parts.append(prompt)
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(None, lambda: self._gemini.generate_content(parts))
        return _parse_json(resp.text)

    async def _gemini_text_call(self, prompt: str) -> str:
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(
            None, lambda: self._gemini_text.generate_content(prompt)
        )
        return resp.text

    async def _groq_call(self, prompt: str, image_data: Optional[bytes], image_mime: str) -> dict:
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
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": content},
            ],
            temperature=0.15,
            max_tokens=500,
        )
        return _parse_json(resp.choices[0].message.content)

    async def _groq_text_call(self, prompt: str) -> str:
        resp = await self._groq.chat.completions.create(
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=1200,
        )
        return resp.choices[0].message.content

    # ── Fallback Formatters (no AI) ────────────────────────────────────────────
    @staticmethod
    def _fallback_briefing(events: list, date_str: str) -> str:
        lines = [f"📅 *TODAY'S HIGH IMPACT NEWS*\n{date_str}\n"]
        for ev in events:
            emoji = "🔴" if ev.get("impact") == "red" else "🟠"
            lines.append(
                f"{emoji} {ev.get('time_12h', '—')} | {ev.get('currency', '—')}: "
                f"{ev.get('name', 'Unknown')}\n"
                f"• Forecast: {ev.get('forecast', '—')} | "
                f"Previous: {ev.get('previous', '—')}"
            )
        lines.append("\n📌 *MARKET STATUS:* Monitor volatility closely during releases.")
        return "\n".join(lines)

    @staticmethod
    def _fallback_alert(event: dict) -> str:
        emoji = "🔴" if event.get("impact") == "red" else "🟠"
        return (
            f"🚨 *ALERT: 10 MINUTES REMAINING*\n\n"
            f"EVENT: {emoji} {event.get('name', 'Unknown Event')}\n"
            f"TIME: {event.get('time_12h', '—')} EAT\n"
            f"FORECAST: {event.get('forecast', '—')}\n"
            f"PREVIOUS: {event.get('previous', '—')}\n\n"
            f"*REQUIRED ACTION:*\n"
            f"✅ Secure open profits\n"
            f"✅ Move Stop-Loss to Break-even\n"
            f"✅ No new entries during release\n\n"
            f"*PROTECT YOUR CAPITAL 💯
        )

    @staticmethod
    def _fallback_weekly(events: list, week_range: str) -> str:
        lines = [f"📅 *WEEKLY HIGH IMPACT OUTLOOK*\nWeek of {week_range}\n"]
        from itertools import groupby
        for day, day_events in groupby(events, key=lambda e: e.get("date", "Unknown")):
            lines.append(f"\n*{day}*")
            for ev in day_events:
                emoji = "🔴" if ev.get("impact") == "red" else "🟠"
                lines.append(
                    f"{emoji} {ev.get('time_12h', '—')} | {ev.get('currency', '—')}: "
                    f"{ev.get('name', 'Unknown')}\n"
                    f"  ↳ Forecast: {ev.get('forecast', '—')} | "
                    f"Previous: {ev.get('previous', '—')}"
                )

        return "\n".join(lines)
