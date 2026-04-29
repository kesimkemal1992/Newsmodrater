"""
ai_engine.py — Dual-layer AI analysis engine.
Geopolitical exception, Groq fallback, trailing year removal.
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

CHANNEL_SIGNATURE = "\n\n[Squad 4xx](https://t.me/Squad_4xx)"
ALLOWED_HASHTAGS = "#XAUUSD #DXY #OIL"

def _add_us_flag_emoji(text: str) -> str:
    if not text:
        return text
    lines = text.split('\n')
    if not lines:
        return text
    first_line = lines[0]
    new_line = re.sub(r'\bUS\b', 'US 🇺🇸', first_line, count=1)
    new_line = re.sub(r'\bUSD\b', 'USD 🇺🇸', new_line, count=1)
    lines[0] = new_line
    return '\n'.join(lines)

_SYSTEM_PROMPT = """
You are AXIOM INTEL — a Senior Institutional Macro & Geopolitical news editor.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔥 GEOPOLITICAL EXCEPTION (ALWAYS APPROVE)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Any statement from a world leader (e.g., Trump, Biden, Putin, Xi) that affects:
- Oil supply (Hormuz, OPEC, embargo, sanctions)
- War / conflict escalation
- Tariffs / trade restrictions
- Central bank or financial policy changes
- Gold, USD, or energy markets
These are HIGH IMPACT geopolitical events, even if posted on social media.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
YOUR ONLY JOB:
Take the source content, verify its relevance, and format it cleanly.
Do NOT speculate. Do NOT add analysis beyond the facts.
Do NOT change the meaning.

CRITICAL FORMATTING RULES:
- DO NOT use asterisks (*) or any markdown bolding
- Use ONLY plain text and emojis
- NO NOTE line. NO MARKET STATUS. NO commentary line.
- NO forecast, NO previous data — never include numbers
- Hashtags: ONLY #XAUUSD #DXY #OIL — no other hashtags
- Do NOT add the current year at the end.
- Do NOT add signature (added automatically)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REJECT IF ANY OF THESE APPLY (EXCEPT the Geopolitical Exception):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. SIGNALS       — Buy/Sell/Long/Short/Entry/TP/SL/price targets
2. CHART / TA    — Technical analysis, patterns, indicators
3. MEME          — Memes, jokes, informal content
4. ANALYSIS IMG  — Chart screenshots, TA images
5. WATERMARK     — Another channel logo or username
6. STALE         — Content older than 18 hours
7. OFF-TOPIC     — Not about geopolitics, central banks, macro data, Gold, Oil, USD
8. LOW VALUE     — Vague, no specific real-world event
9. DUPLICATE     — Same story already processed
10. PREDICTION   — "I think", "expect", "my analysis"
11. COMMENTARY   — Personal views, market opinions

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FORMAT (if approved):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[EMOJI] [SHORT ENGLISH HEADLINE — one line, factual]

[Source content lightly cleaned. 2-4 sentences max. Plain facts only.]

#XAUUSD #DXY #OIL

EMOJI: 🚨 🌍 📊 🏦 🛢️ 🏆 💵 ⚠️ 🗳️

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPOND WITH VALID JSON ONLY — NO MARKDOWN FENCES — NO TRAILING COMMAS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{"approved": true, "reason": "brief reason", "issues": [], "formatted_text": "...", "confidence": 0.9}
""".strip()

_SIMILARITY_PROMPT = """
You are a duplicate news detector. Compare the two stories. If they describe the same real-world event – even if worded differently, in different languages, or with minor spelling mistakes – respond with same_story=true.
Be aggressive. If any reasonable chance they are the same, mark true.

Story A: {story_a}
Story B: {story_b}

Respond in JSON: {{"same_story": true/false, "confidence": 0.0-1.0, "reason": "..."}}
"""

_MULTIMODAL_SIMILARITY_PROMPT = """
You are a duplicate news detector. Compare the two items (text + optional images). Decide if they are the SAME real-world event.

Item A text: {text_a}
Item B text: {text_b}
(Images are compared visually if both exist)

Be aggressive: if there is any reasonable chance they are the same, mark same_story=true.

Respond with JSON: {{"same_story": true, "confidence": 0.0-1.0, "reason": "..."}}
"""

_FF_IMAGE_PROMPT = """
You are analysing a ForexFactory economic calendar screenshot.

TODAY'S DATE: {today_date}

TASKS:
1. Confirm this is a real ForexFactory calendar image (not meme/chart)
2. Confirm it shows TODAY's date – reject otherwise
3. Extract only USD high-impact (Red) and medium-impact (Orange) events
4. Format a clean daily briefing – NO forecast, NO previous data

STRICT RULES:
- Only USD events
- Only 🔴 and 🟠 impact
- Times in 12-hour AM/PM only (no timezone)
- NO forecast values, NO previous values
- NO NOTE line, NO commentary
- Plain text only, no asterisks, no bold
- Do NOT add signature

If not a valid FF image or not today's date, respond:
{{"approved": false, "reason": "not a valid ForexFactory today image"}}

If valid:
{{"approved": true, "reason": "valid FF today image", "formatted_text": "📅 TODAY'S USD HIGH IMPACT NEWS\\nDay, Month DD, YYYY\\n\\n🔴 03:30 PM | USD: Event Name\\n🟠 05:00 PM | USD: Another Event\\n\\nBe careful during these releases."}}
RESPOND WITH VALID JSON ONLY.
"""

_FF_WEEKLY_IMAGE_PROMPT = """
You are analysing a ForexFactory calendar for the weekly outlook.

CURRENT WEEK: {week_range}

Extract USD high-impact (Red) and medium-impact (Orange) events for this week.
Group by day. Times 12-hour AM/PM. NO forecast/previous. Plain text.

If valid respond:
{{"approved": true, "reason": "valid FF weekly image", "formatted_text": "📅 WEEKLY HIGH IMPACT NEWS\\nWeek of {week_range}\\n\\nMonday — Apr 28\\n🔴 03:30 PM | USD: Event Name\\n..."}}
Otherwise {{"approved": false, "reason": "..."}}
"""

_ALERT_PROMPT_TEMPLATE = """
You are a Senior Institutional Trader writing a pre-event warning alert.

STRICT RULES:
- NO asterisks or markdown – plain text only
- English only
- Say NEWS: not EVENT:
- Do NOT include Forecast or Previous
- Do NOT add NOTE or commentary
- Do NOT add signature
- Copy the motivational closing line exactly

News details:
Name: {event_name}
Currency: {currency}
Time: {time_12h}
Impact: {impact_emoji}

Motivational closing line (copy exactly):
{motivational_line}

Write EXACT format:

🚨 ALERT: 10 MINUTES REMAINING

NEWS: {impact_emoji} {event_name}
TIME: {time_12h}

REQUIRED ACTION:
✅ Secure open profits now
✅ Move Stop-Loss to Break-even
✅ No new entries during the release

{motivational_line}

Return ONLY the formatted alert. No JSON. No markdown.
"""

def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()

def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def _add_signature(text: str) -> str:
    text = text.strip()
    if "[Squad 4xx]" not in text:
        text += CHANNEL_SIGNATURE
    return text

_MOTIVATIONAL_POOL = [
    "🛡️ Guard your account like it is your last one — because one day, it might be. Stay safe. 🔒",
    "💰 Your account is everything. One reckless trade during news can erase weeks of work. 🚫",
    "🔒 Do not risk more than you can afford to lose right now. Protect your balance first. 💡",
    "⚠️ Be careful — this is a high-risk moment. Reduce your size or stay out completely. 🛑",
    "🧘 Calm traders keep their accounts. Emotional traders lose them. Breathe and be careful. 💎",
    "📵 Step away from the chart if you feel the urge to gamble. Your account will thank you. 🙏",
    "💳 Treat every dollar in your account as irreplaceable. Getting it back is twice as hard. 📊",
    "🔐 A protected account is a surviving account. A surviving account is a winning account. ✅",
    "🚨 Be careful — news spikes destroy unprotected accounts in seconds. Still be here tomorrow. 📅",
    "🧠 The best thing you can do right now is nothing. Be careful and wait for clarity. ⏳",
    "💵 Never let one news event define your month. Keep your risk small and live to trade again. 🗓️",
    "🛡️ Move your stop to break-even. Lock in your safety. Your account matters more than this trade. 🔑",
    "📉 A 20% loss needs a 25% gain to recover. Be careful — protect what you have. 📈",
    "🚫 Do not add to a losing position during news. That is how accounts go to zero. Be careful. ✋",
    "💡 Traders who protect their accounts during news events are the ones still trading next year. 🏆",
    "⏸️ If you have no stop loss right now — close the trade. No exceptions. Be careful. 🔒",
    "🙅 Revenge trading after a news spike is dangerous. Take a break. Be careful with your account. 🛡️",
    "📌 Account survival is the number one priority. Everything else comes after. Be careful. 💯",
    "🔴 Be careful — one wrong move right now can hurt your account badly. Stay disciplined. ⚠️",
    "💰 You worked hard for every dollar in that account. Be careful and do not give it away. 🛡️",
]

def _get_motivational_line(index: int = 0) -> str:
    return _MOTIVATIONAL_POOL[index % len(_MOTIVATIONAL_POOL)]

def _parse_json(raw: str) -> dict:
    if not raw:
        raise ValueError("Empty response from AI engine.")
    raw = re.sub(r"```+(?:json|JSON)?", "", raw)
    raw = re.sub(r"```+", "", raw)
    raw = raw.strip().strip("`").strip()
    raw = re.sub(r",\s*([}\]])", r"\1", raw)
    try:
        return _validate_and_clean(json.loads(raw))
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        candidate = re.sub(r",\s*([}\]])", r"\1", m.group())
        try:
            return _validate_and_clean(json.loads(candidate))
        except json.JSONDecodeError:
            pass
    log.warning(f"_parse_json failed. Raw snippet: {raw[:200]}")
    raise ValueError(f"No valid JSON found in AI response:\n{raw[:300]}")

def _validate_and_clean(data: dict) -> dict:
    data.setdefault("approved", False)
    data.setdefault("reason", "")
    data.setdefault("issues", [])
    data.setdefault("formatted_text", "")
    data.setdefault("confidence", 0.5)

    if data.get("formatted_text"):
        data["formatted_text"] = data["formatted_text"].replace("*", "")
        data["formatted_text"] = re.sub(r"📌\s*(NOTE|MARKET STATUS|STATUS)[^\n]*\n?", "", data["formatted_text"]).strip()
        if data.get("approved"):
            data["formatted_text"] = re.sub(r"#\w+", "", data["formatted_text"]).strip()

    if data.get("approved") and _signal_hit(data.get("formatted_text", "")):
        log.warning("Signal keyword in output — hard reject.")
        data["approved"] = False
        data["reason"] = "Signal keyword found in output."
        data["issues"].append("signal_content")
        data["formatted_text"] = ""

    return data

def _signal_hit(text: str) -> Optional[str]:
    if not text:
        return None
    _SIGNAL_RE = re.compile(
        r"\b(buy|sell|long|short|entry|tp|take[\s_-]?profit|sl|stop[\s_-]?loss|"
        r"stoploss|stop\s+at\s+\d|entry\s*[:\-]?\s*\d|target\s*[:\-]?\s*\d)\b",
        re.IGNORECASE,
    )
    m = _SIGNAL_RE.search(text)
    return m.group(0).strip() if m else None

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
            generation_config=genai.GenerationConfig(temperature=0.15, max_output_tokens=600, response_mime_type="application/json"),
        )
        self._gemini_text = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config=genai.GenerationConfig(temperature=0.2, max_output_tokens=1200),
        )
        self._gemini_vision = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config=genai.GenerationConfig(temperature=0.1, max_output_tokens=800, response_mime_type="application/json"),
        )

    async def analyse(self, text: str, image_data: Optional[bytes] = None, image_mime: str = "image/jpeg") -> dict:
        prompt = self._build_moderation_prompt(text)
        try:
            verdict = await asyncio.wait_for(self._gemini_call(prompt, image_data, image_mime), timeout=40)
            verdict["engine"] = "gemini-2.5-flash"
            log.info(f"Gemini → approved={verdict['approved']} | {verdict.get('reason', '')}")
            if verdict.get("approved") and verdict.get("formatted_text"):
                verdict["formatted_text"] = _build_post_body(verdict["formatted_text"])
                verdict["formatted_text"] = _add_us_flag_emoji(verdict["formatted_text"])
            return verdict
        except Exception as exc:
            log.warning(f"Gemini failed ({exc}) — trying Groq …")
        try:
            verdict = await asyncio.wait_for(self._groq_call(prompt, image_data, image_mime), timeout=55)
            verdict["engine"] = "groq-llama4-scout"
            log.info(f"Groq → approved={verdict['approved']} | {verdict.get('reason', '')}")
            if verdict.get("approved") and verdict.get("formatted_text"):
                verdict["formatted_text"] = _build_post_body(verdict["formatted_text"])
                verdict["formatted_text"] = _add_us_flag_emoji(verdict["formatted_text"])
            return verdict
        except Exception as exc:
            log.error(f"Both engines failed — safe reject.")
            return _reject("Both AI engines unavailable.", "engine_error", confidence=0.0)

    async def is_same_story(self, text_a: str, text_b: str, image_a: Optional[bytes] = None, image_b: Optional[bytes] = None) -> bool:
        if not text_a and not text_b and not image_a and not image_b:
            return False
        if image_a or image_b:
            prompt = _MULTIMODAL_SIMILARITY_PROMPT.format(
                text_a=(text_a[:400] if text_a else "(no text)"),
                text_b=(text_b[:400] if text_b else "(no text)"),
            )
        else:
            prompt = _SIMILARITY_PROMPT.format(
                story_a=(text_a[:500] if text_a else ""),
                story_b=(text_b[:500] if text_b else ""),
            )
        try:
            parts = []
            if image_a:
                parts.append({"inline_data": {"mime_type": "image/jpeg", "data": _b64(image_a)}})
            if image_b:
                parts.append({"inline_data": {"mime_type": "image/jpeg", "data": _b64(image_b)}})
            parts.append(prompt)
            loop = asyncio.get_event_loop()
            resp = await asyncio.wait_for(loop.run_in_executor(None, lambda: self._gemini_vision.generate_content(parts)), timeout=20)
            data = _parse_json(resp.text)
            same = bool(data.get("same_story", False))
            conf = data.get("confidence", 0)
            log.info(f"Gemini similarity → same={same} | conf={conf}")
            return same and conf >= 0.55
        except Exception as exc:
            log.warning(f"Gemini similarity failed ({exc}) — trying Groq …")
        try:
            content = []
            if image_a:
                content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{_b64(image_a)}"}})
            if image_b:
                content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{_b64(image_b)}"}})
            content.append({"type": "text", "text": prompt})
            resp = await asyncio.wait_for(
                self._groq.chat.completions.create(
                    model="meta-llama/llama-4-scout-17b-16e-instruct",
                    messages=[{"role": "user", "content": content}],
                    temperature=0.1,
                    max_tokens=300,
                ),
                timeout=25,
            )
            data = _parse_json(resp.choices[0].message.content)
            same = bool(data.get("same_story", False))
            conf = data.get("confidence", 0)
            log.info(f"Groq similarity → same={same} | conf={conf}")
            return same and conf >= 0.55
        except Exception as exc:
            log.error(f"Both engines failed for similarity check: {exc}")
            return False

    async def analyse_ff_image(self, image_data: bytes, image_mime: str, today_date: str,
                               is_weekly: bool = False, week_range: str = "") -> dict:
        if is_weekly:
            prompt = _FF_WEEKLY_IMAGE_PROMPT.format(week_range=week_range)
        else:
            prompt = _FF_IMAGE_PROMPT.format(today_date=today_date)
        parts = [{"inline_data": {"mime_type": image_mime, "data": _b64(image_data)}}, prompt]
        try:
            loop = asyncio.get_event_loop()
            resp = await asyncio.wait_for(loop.run_in_executor(None, lambda: self._gemini_vision.generate_content(parts)), timeout=45)
            data = _parse_json(resp.text)
            log.info(f"FF image → approved={data.get('approved')} | {data.get('reason', '')}")
            if data.get("approved") and data.get("formatted_text"):
                data["formatted_text"] = _add_us_flag_emoji(data["formatted_text"])
            return data
        except Exception as exc:
            log.warning(f"Gemini FF failed ({exc}) — trying Groq …")
        try:
            content = [
                {"type": "image_url", "image_url": {"url": f"data:{image_mime};base64,{_b64(image_data)}"}},
                {"type": "text", "text": prompt},
            ]
            resp = await asyncio.wait_for(
                self._groq.chat.completions.create(
                    model="meta-llama/llama-4-scout-17b-16e-instruct",
                    messages=[{"role": "user", "content": content}],
                    temperature=0.1,
                    max_tokens=800,
                ),
                timeout=60,
            )
            data = _parse_json(resp.choices[0].message.content)
            log.info(f"Groq FF → approved={data.get('approved')}")
            if data.get("approved") and data.get("formatted_text"):
                data["formatted_text"] = _add_us_flag_emoji(data["formatted_text"])
            return data
        except Exception as exc:
            log.error(f"Both engines failed for FF image: {exc}")
            return {"approved": False, "reason": "AI engines unavailable for image analysis."}

    async def generate_alert(self, event: dict, motivational_index: int = 0) -> str:
        impact_emoji = "🔴" if event.get("impact") == "red" else "🟠"
        motivational_line = _get_motivational_line(motivational_index)
        prompt = _ALERT_PROMPT_TEMPLATE.format(
            event_name=event.get("name", "Unknown Event"),
            currency=event.get("currency", "USD"),
            time_12h=event.get("time_12h", "—"),
            impact_emoji=impact_emoji,
            motivational_line=motivational_line,
        )
        try:
            result = await asyncio.wait_for(self._gemini_text_call(prompt), timeout=30)
            log.info(f"Alert generated for: {event.get('name')}")
            result = _add_us_flag_emoji(result)
            return _add_signature(_strip_asterisks(result.strip()))
        except Exception as exc:
            log.warning(f"Gemini alert failed ({exc}) — trying Groq …")
        try:
            result = await asyncio.wait_for(self._groq_text_call(prompt), timeout=45)
            result = _add_us_flag_emoji(result)
            return _add_signature(_strip_asterisks(result.strip()))
        except Exception as exc:
            log.error(f"Both engines failed for alert — using fallback.")
            return self._fallback_alert(event, motivational_index)

    def _build_moderation_prompt(self, text: str) -> str:
        return textwrap.dedent(f"""
            DATE (UTC): {_today_str()}
            CHANNEL FOCUS: {self._category}
            SOURCE CONTENT:
            \"\"\"
            {text.strip() if text else "(image only — no text)"}
            \"\"\"
            TASK: Analyse content. If it is relevant geopolitical/macro news (Gold, Oil, USD, central banks, geopolitics, energy, political leaders) approve and format. If signal, TA, meme, off‑topic, low‑value, stale – reject. Format according to rules. Return JSON.
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
            temperature=0.15, max_tokens=600,
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
    def _fallback_alert(event: dict, motivational_index: int = 0) -> str:
        emoji = "🔴" if event.get("impact") == "red" else "🟠"
        line = _get_motivational_line(motivational_index)
        text = (
            f"🚨 ALERT: 10 MINUTES REMAINING\n\n"
            f"NEWS: {emoji} {event.get('name', 'Unknown Event')}\n"
            f"TIME: {event.get('time_12h', '—')}\n\n"
            f"REQUIRED ACTION:\n"
            f"✅ Secure open profits now\n"
            f"✅ Move Stop-Loss to Break-even\n"
            f"✅ No new entries during the release\n\n"
            f"{line}"
        )
        text = _add_us_flag_emoji(text)
        return _add_signature(text)

def _reject(reason: str, issue: str, confidence: float = 1.0) -> dict:
    return {
        "approved": False,
        "reason": reason,
        "issues": [issue],
        "formatted_text": "",
        "confidence": confidence,
        "engine": "pre_filter",
    }

def _build_post_body(text: str) -> str:
    if not text:
        return ""
    text = text.replace("*", "")
    text = re.sub(r"📌\s*(NOTE|MARKET STATUS|STATUS)[^\n]*\n?", "", text).strip()
    text = re.sub(r"#\w+", "", text).strip()
    text = f"{text}\n\n{ALLOWED_HASHTAGS}"
    text = _add_signature(text)
    text = re.sub(r'\s+(\d{4})$', '', text.strip())
    return text
