"""
ai_engine.py — Dual-layer AI analysis engine.

Primary  : Gemini 2.5 Flash  (google-generativeai)
Fallback : Groq llama-4-scout (vision capable)

Style: Senior Institutional Trader — English only, minimalist, direct.
       NO asterisks. NO markdown bolding. Data-first. 12-hour AM/PM time format.
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

# ─── News Moderation System Prompt ────────────────────────────────────────────
_SYSTEM_PROMPT = """
You are AXIOM INTEL — a Senior Institutional Macro & Geopolitical news editor
for a professional Telegram trading channel. Your audience is experienced traders.

YOUR ONLY JOB:
Take the source content, verify its relevance, and format it cleanly.
Do NOT speculate. Do NOT add analysis beyond the facts.
Do NOT change the meaning. Format it professionally and precisely.

CRITICAL FORMATTING RULE:
DO NOT use asterisks (*) or any markdown bolding anywhere in the output.
Use ONLY plain text, emojis, and hashtags. No bold. No italic. No markdown.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REJECT IF ANY OF THESE APPLY:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. SIGNALS       — Buy / Sell / Long / Short / Entry / TP / SL / price targets
2. CHART ONLY    — Image with no news context
3. WATERMARK     — Another channel logo or username on image
4. STALE         — Content older than 18 hours
5. OFF-TOPIC     — Not about geopolitics, central banks, CPI/NFP/GDP,
                   Gold, Oil, major FX pairs, interest rates
6. LOW VALUE     — Vague, no specific event or data point
7. DUPLICATE     — Essentially same content already processed

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FORMAT (if approved):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[EMOJI] [SHORT ENGLISH HEADLINE — one line, factual and direct]

[Source content lightly cleaned. English only. NO bold. NO asterisks.
2-4 sentences max. Do not add anything new.]

📌 NOTE: [ONE short line on likely market impact — plain text only,
   no asterisks, e.g. "Focus: USD liquidity expansion ahead of FOMC." or
   "Gold safe-haven demand elevated on geopolitical risk."]

#PAIR1 #PAIR2

RULES:
• English ONLY. No Amharic or other languages.
• ZERO asterisks anywhere — not for bold, not for any reason
• Plain text only for all content
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
RESPOND WITH VALID JSON ONLY — NO MARKDOWN FENCES — NO TRAILING COMMAS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{"approved": true, "reason": "brief reason", "issues": [], "formatted_text": "post here", "hashtags": "#PAIR1", "confidence": 0.9}
""".strip()

# ─── Daily Briefing Prompt ─────────────────────────────────────────────────────
_BRIEFING_PROMPT_TEMPLATE = """
You are a Senior Institutional Trader writing a morning economic calendar briefing.

STRICT RULES:
- DO NOT use asterisks (*) or any markdown bolding anywhere
- Plain text and emojis ONLY
- English only
- Times in 12-hour AM/PM format (Africa/Addis_Ababa GMT+3)
- If Forecast or Previous is unknown, write a dash (—)
- Sort events chronologically
- Only include Red (🔴) and Orange (🟠) impact events

Today's high-impact events (JSON):
{events_json}

Date: {date_str}

Write the briefing in this EXACT format (no asterisks anywhere):

📅 TODAY'S HIGH IMPACT NEWS
{date_str}

🔴 03:30 PM | USD: Event Name
• Forecast: 0.3% | Previous: 0.4%

🟠 05:00 PM | EUR: Another Event
• Forecast: — | Previous: 1.2%

📌 NOTE: [One plain-text sentence on expected volatility or market theme]

Return ONLY the formatted message. No JSON. No markdown. No asterisks.
""".strip()

# ─── 10-Minute Alert Prompt ────────────────────────────────────────────────────
_ALERT_PROMPT_TEMPLATE = """
You are a Senior Institutional Trader writing a pre-event warning alert.

STRICT RULES:
- DO NOT use asterisks (*) or any markdown bolding — plain text only
- English only
- No NOTE section
- The motivational closing line is already provided below — copy it exactly, do not change it
- FORECAST line: only include if {has_forecast} is true
- PREVIOUS line: only include if {has_previous} is true
- If both are missing, skip both lines entirely — do not write dashes or empty lines

Event details:
Name: {event_name}
Currency: {currency}
Time: {time_12h} EAT
Forecast: {forecast}
Previous: {previous}
Has Forecast: {has_forecast}
Has Previous: {has_previous}
Impact: {impact_emoji}

Motivational closing line (copy exactly, do not modify):
{motivational_line}

Write in this EXACT format (skip FORECAST/PREVIOUS lines if not available):

🚨 ALERT: 10 MINUTES REMAINING

EVENT: {impact_emoji} {event_name}
TIME: {time_12h} EAT
[FORECAST: {forecast}   ← only if has_forecast is true]
[PREVIOUS: {previous}   ← only if has_previous is true]

REQUIRED ACTION:
✅ Secure open profits now
✅ Move Stop-Loss to Break-even
✅ No new entries during the release

{motivational_line}

Return ONLY the formatted alert. No JSON. No markdown. No asterisks.
""".strip()

# ─── Weekly Outlook Prompt ─────────────────────────────────────────────────────
_WEEKLY_PROMPT_TEMPLATE = """
You are a Senior Institutional Trader writing a Sunday weekly economic outlook.

STRICT RULES:
- DO NOT use asterisks (*) or any markdown bolding — plain text only
- Use emojis for structure and visual clarity
- English only
- Times in 12-hour AM/PM format (Africa/Addis_Ababa GMT+3)
- If Forecast or Previous is unknown, write a dash (—)
- Group events by day, sorted chronologically
- Only Red (🔴) and Orange (🟠) events

Events this week (JSON):
{events_json}

Week: {week_range}

Write in this EXACT format (no asterisks anywhere):

📅 WEEKLY HIGH IMPACT OUTLOOK
Week of {week_range}

Monday — [Date]
🔴 [time] | [currency]: [event name]
  ↳ Forecast: [value] | Previous: [value]

Tuesday — [Date]
🟠 [time] | [currency]: [event name]
  ↳ Forecast: [value] | Previous: [value]

[Continue for all days that have events]

📌 NOTE:
[2-3 plain text sentences. Factual institutional summary.
No asterisks. Mention the most critical events and market themes.]

Return ONLY the formatted message. No JSON. No markdown. No asterisks.
""".strip()


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ─── Rotating Motivational Closer Pool ────────────────────────────────────────
_MOTIVATIONAL_POOL = [
    "🛡️ Guard your account like it is your last one — because one day, it might be. Stay safe. 🔒",
    "💰 Your account is everything. One reckless trade during news can erase weeks of hard work. 🚫",
    "🔒 Do not risk more than you can afford to lose right now. Protect your balance first. 💡",
    "⚠️ This is a high-risk moment. Reduce your size, tighten your stop, or stay out completely. 🛑",
    "🧘 Calm traders keep their accounts. Emotional traders lose them. Breathe and do not overtrade. 💎",
    "📵 Step away from the chart if you feel the urge to gamble. Your account will thank you. 🙏",
    "💳 Treat every dollar in your account as irreplaceable. Because getting it back is twice as hard. 📊",
    "🔐 A protected account is a surviving account. A surviving account is a winning account. ✅",
    "🚨 News volatility destroys unprotected accounts in seconds. Be the trader who is still here tomorrow. 📅",
    "🧠 The best thing you can do right now is nothing. Protect your account and wait for clarity. ⏳",
    "💵 Never let one news event define your month. Keep your risk small and live to trade again. 🗓️",
    "🛡️ Move your stop to break-even. Lock in your safety. Your account is more important than this trade. 🔑",
    "📉 A 20% account loss needs a 25% gain to recover. Protect what you have — it is harder to earn back. 📈",
    "🚫 Do not add to a losing position during news. That is how accounts go to zero. Stay disciplined. ✋",
    "💡 The traders who protect their accounts during news events are the ones still trading next year. 🏆",
    "⏸️ If you have no stop loss right now — close the trade. No exceptions. Protect your account first. 🔒",
    "🙅 Revenge trading after a news spike is account suicide. Take a break. Your money needs protection. 🛡️",
    "📌 Write this down: Account survival is the number one priority. Everything else comes after. 💯",
    "🔴 High impact event active. One wrong move without protection can hurt your account badly. Stay careful. ⚠️",
    "💰 You worked hard for every dollar in that account. Do not hand it to the market carelessly. Guard it. 🛡️",
]


def _get_motivational_line(index: int = 0) -> str:
    return _MOTIVATIONAL_POOL[index % len(_MOTIVATIONAL_POOL)]


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
    if not raw:
        raise ValueError("Empty response from AI engine.")
    raw = re.sub(r"```+(?:json|JSON)?", "", raw)
    raw = re.sub(r"```+", "", raw)
    raw = raw.strip().strip("`").strip()
    raw = re.sub(r",\s*([}\]])", r"\1", raw)
    try:
        data = json.loads(raw)
        return _validate_and_clean(data)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        candidate = m.group()
        candidate = re.sub(r",\s*([}\]])", r"\1", candidate)
        try:
            data = json.loads(candidate)
            return _validate_and_clean(data)
        except json.JSONDecodeError:
            pass
    log.warning(f"_parse_json: all strategies failed. Raw snippet: {raw[:200]}")
    raise ValueError(f"No valid JSON found in AI response:\n{raw[:300]}")


def _validate_and_clean(data: dict) -> dict:
    data.setdefault("approved", False)
    data.setdefault("reason", "")
    data.setdefault("issues", [])
    data.setdefault("formatted_text", "")
    data.setdefault("hashtags", "")
    data.setdefault("confidence", 0.5)
    if data.get("formatted_text"):
        data["formatted_text"] = data["formatted_text"].replace("*", "")
    if data.get("approved") and _signal_hit(data.get("formatted_text", "")):
        log.warning("Signal keyword detected in output — hard reject.")
        data["approved"] = False
        data["reason"] = "Signal keyword found in formatted output."
        data["issues"].append("signal_content")
        data["formatted_text"] = ""
        data["hashtags"] = ""
    return data


def _strip_asterisks(text: str) -> str:
    return text.replace("*", "") if text else text


class AIEngine:
    def __init__(self, gemini_key: str, groq_key: str, channel_category: str):
        self._category = channel_category
        self._groq = AsyncGroq(api_key=groq_key)
        genai.configure(api_key=gemini_key)
        self._gemini = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            system_instruction=_SYSTEM_PROMPT,
            generation_config=genai.GenerationConfig(
                temperature=0.15,
                max_output_tokens=500,
                response_mime_type="application/json",
            ),
        )
        self._gemini_text = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config=genai.GenerationConfig(
                temperature=0.2,
                max_output_tokens=1200,
            ),
        )

    async def analyse(self, text: str, image_data: Optional[bytes] = None, image_mime: str = "image/jpeg") -> dict:
        hit = _signal_hit(text)
        if hit:
            log.info(f"[PRE-FILTER] Signal keyword '{hit}' — instant reject.")
            return {"approved": False, "reason": f"Signal keyword detected: '{hit}'", "issues": ["signal_content"], "formatted_text": "", "hashtags": "", "confidence": 1.0, "engine": "pre_filter"}
        prompt = self._build_moderation_prompt(text)
        try:
            verdict = await asyncio.wait_for(self._gemini_call(prompt, image_data, image_mime), timeout=40)
            verdict["engine"] = "gemini-2.5-flash"
            log.info(f"Gemini NEWS → approved={verdict['approved']} | {verdict.get('reason', '')}")
            return verdict
        except Exception as exc:
            log.warning(f"Gemini NEWS failed ({exc}) — trying Groq …")
        try:
            verdict = await asyncio.wait_for(self._groq_call(prompt, image_data, image_mime), timeout=55)
            verdict["engine"] = "groq-llama4-scout"
            log.info(f"Groq NEWS → approved={verdict['approved']} | {verdict.get('reason', '')}")
            return verdict
        except Exception as exc:
            log.error(f"Groq NEWS failed ({exc}) — safe reject.")
            return {"approved": False, "reason": "Both AI engines unavailable.", "issues": ["engine_error"], "formatted_text": "", "hashtags": "", "confidence": 0.0, "engine": "none"}

    async def generate_daily_briefing(self, events: list, date_str: str) -> str:
        if not events:
            return ""
        prompt = _BRIEFING_PROMPT_TEMPLATE.format(events_json=json.dumps(events, ensure_ascii=False, indent=2), date_str=date_str)
        try:
            result = await asyncio.wait_for(self._gemini_text_call(prompt), timeout=45)
            log.info("Daily briefing generated via Gemini.")
            return _strip_asterisks(result.strip())
        except Exception as exc:
            log.warning(f"Gemini briefing failed ({exc}) — trying Groq …")
        try:
            result = await asyncio.wait_for(self._groq_text_call(prompt), timeout=60)
            log.info("Daily briefing generated via Groq fallback.")
            return _strip_asterisks(result.strip())
        except Exception as exc:
            log.error(f"Both engines failed for briefing — using fallback formatter.")
            return self._fallback_briefing(events, date_str)

    async def generate_alert(self, event: dict, motivational_index: int = 0) -> str:
        impact_emoji = "🔴" if event.get("impact") == "red" else "🟠"
        motivational_line = _get_motivational_line(motivational_index)

        # Determine if Forecast / Previous are real values
        forecast_raw = event.get("forecast", "").strip()
        previous_raw = event.get("previous", "").strip()
        has_forecast = bool(forecast_raw and forecast_raw != "—")
        has_previous = bool(previous_raw and previous_raw != "—")
        forecast_val = forecast_raw if has_forecast else ""
        previous_val = previous_raw if has_previous else ""

        prompt = _ALERT_PROMPT_TEMPLATE.format(
            event_name=event.get("name", "Unknown Event"), currency=event.get("currency", "USD"),
            time_12h=event.get("time_12h", "—"),
            forecast=forecast_val, previous=previous_val,
            has_forecast=has_forecast, has_previous=has_previous,
            impact=event.get("impact", "red"),
            impact_emoji=impact_emoji, motivational_line=motivational_line,
        )
        try:
            result = await asyncio.wait_for(self._gemini_text_call(prompt), timeout=30)
            log.info(f"Alert generated for: {event.get('name')}")
            return _strip_asterisks(result.strip())
        except Exception as exc:
            log.warning(f"Gemini alert failed ({exc}) — trying Groq …")
        try:
            result = await asyncio.wait_for(self._groq_text_call(prompt), timeout=45)
            return _strip_asterisks(result.strip())
        except Exception as exc:
            log.error(f"Both engines failed for alert — using fallback.")
            return self._fallback_alert(event, motivational_index)

    async def generate_weekly_outlook(self, events: list, week_range: str) -> str:
        if not events:
            return ""
        prompt = _WEEKLY_PROMPT_TEMPLATE.format(events_json=json.dumps(events, ensure_ascii=False, indent=2), week_range=week_range)
        try:
            result = await asyncio.wait_for(self._gemini_text_call(prompt), timeout=60)
            log.info("Weekly outlook generated via Gemini.")
            return _strip_asterisks(result.strip())
        except Exception as exc:
            log.warning(f"Gemini weekly failed ({exc}) — trying Groq …")
        try:
            result = await asyncio.wait_for(self._groq_text_call(prompt), timeout=75)
            log.info("Weekly outlook generated via Groq fallback.")
            return _strip_asterisks(result.strip())
        except Exception as exc:
            log.error(f"Weekly outlook generation completely failed: {exc}")
            return self._fallback_weekly(events, week_range)

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
               Add a NOTE line. Use 12-hour AM/PM times.
               DO NOT use asterisks or markdown bolding.
            4. Return valid JSON only — no markdown fences — no trailing commas.
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
        resp = await loop.run_in_executor(None, lambda: self._gemini_text.generate_content(prompt))
        return resp.text

    async def _groq_call(self, prompt: str, image_data: Optional[bytes], image_mime: str) -> dict:
        content = []
        if image_data:
            content.append({"type": "image_url", "image_url": {"url": f"data:{image_mime};base64,{_b64(image_data)}"}})
        content.append({"type": "text", "text": prompt})
        resp = await self._groq.chat.completions.create(
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            messages=[{"role": "system", "content": _SYSTEM_PROMPT}, {"role": "user", "content": content}],
            temperature=0.15, max_tokens=500,
        )
        return _parse_json(resp.choices[0].message.content)

    async def _groq_text_call(self, prompt: str) -> str:
        resp = await self._groq.chat.completions.create(
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2, max_tokens=1200,
        )
        return resp.choices[0].message.content

    @staticmethod
    def _fallback_briefing(events: list, date_str: str) -> str:
        lines = [f"📅 TODAY'S HIGH IMPACT NEWS\n{date_str}\n"]
        for ev in events:
            emoji = "🔴" if ev.get("impact") == "red" else "🟠"
            lines.append(f"{emoji} {ev.get('time_12h', '—')} | {ev.get('currency', '—')}: {ev.get('name', 'Unknown')}\n• Forecast: {ev.get('forecast', '—')} | Previous: {ev.get('previous', '—')}")
        lines.append("\n📌 NOTE: Monitor volatility closely during all releases.")
        return "\n".join(lines)

    @staticmethod
    def _fallback_alert(event: dict, motivational_index: int = 0) -> str:
        emoji = "🔴" if event.get("impact") == "red" else "🟠"
        line = _get_motivational_line(motivational_index)

        # Only show FORECAST / PREVIOUS lines when values are real
        forecast_raw = event.get("forecast", "").strip()
        previous_raw = event.get("previous", "").strip()
        data_lines = ""
        if forecast_raw and forecast_raw != "—":
            data_lines += f"FORECAST: {forecast_raw}\n"
        if previous_raw and previous_raw != "—":
            data_lines += f"PREVIOUS: {previous_raw}\n"

        return (
            f"🚨 ALERT: 10 MINUTES REMAINING\n\n"
            f"EVENT: {emoji} {event.get('name', 'Unknown Event')}\n"
            f"TIME: {event.get('time_12h', '—')} EAT\n"
            f"{data_lines}"
            f"\nREQUIRED ACTION:\n"
            f"✅ Secure open profits now\n"
            f"✅ Move Stop-Loss to Break-even\n"
            f"✅ No new entries during the release\n\n"
            f"{line}"
        )

    @staticmethod
    def _fallback_weekly(events: list, week_range: str) -> str:
        from itertools import groupby
        lines = [f"📅 WEEKLY HIGH IMPACT OUTLOOK\nWeek of {week_range}\n"]
        for day, day_events in groupby(events, key=lambda e: e.get("date", "Unknown")):
            lines.append(f"\n{day}")
            for ev in day_events:
                emoji = "🔴" if ev.get("impact") == "red" else "🟠"
                lines.append(f"{emoji} {ev.get('time_12h', '—')} | {ev.get('currency', '—')}: {ev.get('name', 'Unknown')}\n  ↳ Forecast: {ev.get('forecast', '—')} | Previous: {ev.get('previous', '—')}")
        lines.append("\n📌 NOTE:\nMonitor all Red folder events closely. Manage risk carefully around high-impact releases.")
        return "\n".join(lines)
