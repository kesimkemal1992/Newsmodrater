"""
ai_engine.py — Final version.
- Calendar posts: NO hashtags, no year, only USD, 12‑hour AM/PM.
- Rejects calendar images with >3 non‑USD events.
- Geopolitical/FOMC exceptions for regular news.
- US flag emoji for news posts.
- Professional motivational lines for reminders.
"""

import asyncio
import base64
import json
import logging
import random
import re
import textwrap
from datetime import datetime, timezone
from typing import Optional

import google.generativeai as genai
from groq import AsyncGroq

log = logging.getLogger("ai_engine")

# Base signature without bulb (bulb added randomly later)
CHANNEL_SIGNATURE = "\n\n[Squad 4xx](https://t.me/Squad_4xx)"
ALLOWED_HASHTAGS_SET = {"#XAUUSD", "#DXY", "#OIL"}

def _add_us_flag_emoji(text: str) -> str:
    """Add US flag emoji after first occurrence of US or USD in headline."""
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

def _add_signature(text: str) -> str:
    """Append channel signature, randomly adding 💡 emoji with 30% probability."""
    text = text.strip()
    if "[Squad 4xx]" not in text:
        if random.random() < 0.3:
            signature = "\n\n💡 " + CHANNEL_SIGNATURE.lstrip("\n\n")
        else:
            signature = CHANNEL_SIGNATURE
        text += signature
    return text

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
🔥 FOMC / CENTRAL BANK EXCEPTION (ALWAYS APPROVE)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Any official announcement or news about:
- Federal Open Market Committee (FOMC)
- Federal Funds Rate / Interest Rate Decision
- Fed Chair Powell speech
- FOMC Statement or Minutes
These are HIGH IMPACT macroeconomic events. Always approve even if they contain numbers like "rate at 5.25%". Do NOT reject as "forecast" or "commentary".

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
YOUR ONLY JOB:
Take the source content, verify its relevance, and format it cleanly.
Do NOT speculate. Do NOT add analysis beyond the facts.
Do NOT change the meaning.

CRITICAL FORMATTING RULES:
- DO NOT use asterisks (*) or any markdown bolding
- Use ONLY plain text and emojis
- NO NOTE line. NO MARKET STATUS. NO commentary line.
- **Actual released figures (e.g., "came at 2.5%", "rose to 2.5%", "was 2.5%") are ALLOWED.**
- **Forecast (expected) and previous values are FORBIDDEN.** Never include them.
- **Technical analysis, signals, predictions, opinions are FORBIDDEN.**
- **Hashtags: Only use #XAUUSD, #DXY, or #OIL – only those relevant to the story.**
  - If the story affects Gold, add #XAUUSD.
  - If it affects USD/FX, add #DXY.
  - If it affects Oil, add #OIL.
  - You may add one, two, or all three, but never add any other hashtag.
- Do NOT add the current year at the end of posts.
- Do NOT add signature (added automatically).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REJECT IF ANY OF THESE APPLY (EXCEPT the Geopolitical/FOMC Exceptions):
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
10. PREDICTION   — "I think", "expect", "my analysis" (but actual results are fine)
11. COMMENTARY   — Personal views, market opinions
12. FORECAST/PREVIOUS — Any mention of "forecast", "expected", "previous" values

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FORMAT (if approved):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[EMOJI] [SHORT ENGLISH HEADLINE — one line, factual]

[Source content lightly cleaned. 2-4 sentences max.
Actual numbers are allowed, but never show forecast or previous values.]

[Relevant hashtags from the set #XAUUSD #DXY #OIL – only those that apply]

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

# ─── FOREXFACTORY PROMPTS (ONLY USD, 12‑hour AM/PM, NO YEAR, NO HASHTAGS) ────
_FF_IMAGE_PROMPT = """
You are analysing a ForexFactory economic calendar screenshot.

TODAY'S DATE: {today_date}  (the year is provided but DO NOT include it in the output)

TASKS:
1. Confirm this is a real ForexFactory calendar image (not meme/chart).
2. Confirm it shows TODAY's date – reject otherwise.
3. **Count the number of non‑USD event rows** visible (events where the currency is not USD, e.g., EUR, GBP, JPY, etc.).
   - If there are **more than 3** non‑USD events → reject the image.
   - If there are 0–3 non‑USD events, proceed.
4. Extract **only USD high‑impact (Red 🔴) and medium‑impact (Orange 🟠) events** visible.
5. **Keep the original time as shown in the screenshot** – do NOT convert time zones.
6. Convert the time to **12‑hour AM/PM** format if needed. Do NOT show 24‑hour times.
7. Format a clean daily briefing – **NO forecast, NO previous data**.
8. **DO NOT include the year** in the date line (only day and month, e.g., "Wednesday, April 29").
9. **DO NOT add any hashtags** – no #XAUUSD, no #DXY, no #OIL, absolutely none.

STRICT RULES:
- Only USD events in the final formatted text.
- Only 🔴 and 🟠 impact.
- Times in 12‑hour AM/PM, no timezone label.
- NO forecast values, NO previous values.
- NO NOTE line, NO commentary.
- Plain text only, no asterisks, no bold.
- Do NOT add signature.

If the image is not a valid ForexFactory calendar, or if it shows more than 3 non‑USD events, respond with:
{{"approved": false, "reason": "not a valid ForexFactory today image (too many other currencies or not FF)"}}

If valid (0–3 non‑USD events), respond with JSON like (note: no hashtags, no year):
{{"approved": true, "reason": "valid FF today image", "formatted_text": "📅 TODAY'S USD HIGH IMPACT NEWS\\nWednesday, April 29\\n\\n🔴 03:30 PM | USD: Non-Farm Payrolls\\n🟠 05:00 PM | USD: ISM PMI\\n\\nBe careful during these releases."}}

RESPOND WITH VALID JSON ONLY.
""".strip()

_FF_WEEKLY_IMAGE_PROMPT = """
You are analysing a ForexFactory calendar for the weekly outlook.
No conversion of time zones. Use 12‑hour AM/PM only.
Only USD high‑impact (🔴) and medium‑impact (🟠) events.
**NO forecast, NO previous data.**
**Do NOT include the year** in dates (use "Monday — Apr 28").
**DO NOT add any hashtags** – no #XAUUSD, no #DXY, no #OIL.

CURRENT WEEK: {week_range}  (year provided but do not output it)

Extract events, group by day, format as 12‑hour AM/PM.
Plain text, no asterisks, no bold. Do not add signature.

If valid example:
{{"approved": true, "reason": "valid FF weekly image", "formatted_text": "📅 WEEKLY HIGH IMPACT NEWS\\nWeek of Apr 28 – May 2\\n\\nMonday — Apr 28\\n🔴 03:30 PM | USD: Event Name\\n\\nBe careful during these releases."}}
Otherwise {{"approved": false, "reason": "..."}}
RESPOND WITH VALID JSON ONLY.
""".strip()

# Alert template without "NEWS:" header – professional style
_ALERT_PROMPT_TEMPLATE = """
You are a Senior Institutional Trader writing a pre-event warning alert.

STRICT RULES:
- NO asterisks or markdown – plain text only
- English only
- Do NOT include "NEWS:" or any similar label
- Do NOT include Forecast or Previous
- Do NOT add NOTE or commentary
- Do NOT add signature
- The alert must be short, direct, and focused on capital protection.

Event details:
Name: {event_name}
Time: {time_12h}
Impact: {impact_emoji}
Minutes left: {minutes_left}

Motivational line (copy exactly):
{motivational_line}

Write EXACT format:

🚨 ALERT: {minutes_left} MINUTES REMAINING

{impact_emoji} {event_name}
🕒 {time_12h}

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

# ========== PROFESSIONAL MOTIVATIONAL LINES ==========
def _get_motivational_line(event_name: str = "", fallback_index: int = 0) -> str:
    """
    Returns a motivational line tailored to the event type.
    All lines are professional, capital-protection focused.
    """
    name_lower = event_name.lower()
    
    # FOMC / Fed related
    if any(kw in name_lower for kw in ["fomc", "federal funds", "interest rate", "fed chair", "powell", "federal reserve"]):
        return "🏦 The Fed holds the keys – rate decisions can trigger violent moves. Protect your capital and respect the uncertainty."
    
    # Non-Farm Payrolls / Jobs
    if any(kw in name_lower for kw in ["non-farm", "nfp", "employment change", "payrolls", "jobless", "unemployment"]):
        return "📊 Jobs data regularly causes 50‑100 pip spikes. Don't gamble – secure profits and wait for clarity."
    
    # CPI / PCE / Inflation
    if any(kw in name_lower for kw in ["cpi", "consumer price", "inflation", "pce"]):
        return "📈 Inflation surprises can shred stops in seconds. Be extremely cautious – reduce size or stay flat."
    
    # GDP
    if "gdp" in name_lower:
        return "📉 GDP releases often create sharp two‑way reversals. Tighten your risk management or stay aside."
    
    # Geopolitical / Oil / War / Trump
    if any(kw in name_lower for kw in ["oil", "hormuz", "war", "iran", "trump", "geopolitical", "missile", "attack", "strike"]):
        return "🌍 Geopolitical spikes are fast and unforgiving. Move to safety – no new entries, protect what you have."
    
    # Fallback pool – rotating, still professional
    general_pool = [
        "🛡️ Market volatility ahead. Guard your account like it's irreplaceable – because it is. Stay safe. 🔒",
        "💰 One reckless trade during high‑impact news can erase weeks of gains. Be disciplined.",
        "🔒 High uncertainty – reduce size or stay out completely. Your first job is capital preservation.",
        "⚠️ This release is known for whipsaws. Move stops to break‑even and wait for the dust to settle.",
        "🧘 Professional traders protect first, chase later. Be calm and cautious.",
        "📵 Step away from the screen if you feel the urge to gamble. Your account will thank you.",
        "💳 Every dollar in your account is hard‑earned. Do not give it away on unpredictable spikes.",
        "🔐 A protected account is a surviving account. Surviving accounts eventually win.",
        "🚨 News spikes destroy unprepared accounts in seconds. Stay careful and live to trade another day.",
        "🧠 The best action during uncertainty is often no action at all. Wait for clarity.",
        "💵 Never let one news event define your month. Keep risk small and trade another day.",
        "🛡️ Lock in safety: move stops to break‑even, reduce leverage, and stay alert.",
        "📉 A 20% loss requires a 25% gain to recover. Protect what you have.",
        "🚫 Never add to a losing position during news. This is how accounts go to zero.",
        "💡 Traders who protect their capital during news events are the ones still trading next year.",
        "⏸️ If you have no stop loss, close the trade now. No exceptions.",
        "🙅 Revenge trading after a spike is dangerous. Take a break. Protect your account.",
        "📌 Account survival is the number one priority. Everything else is secondary.",
        "🔴 Be careful – one wrong move right now can hurt your account badly. Stay disciplined.",
        "💰 You worked hard for every dollar. Do not give it away to unpredictable volatility.",
    ]
    return general_pool[fallback_index % len(general_pool)]

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
        text = data["formatted_text"]
        if "TODAY'S USD HIGH IMPACT" in text or "WEEKLY HIGH IMPACT" in text:
            text = re.sub(r"#\w+", "", text).strip()
            data["formatted_text"] = text
        else:
            hashtags = re.findall(r"#\w+", text)
            allowed_hashtags = [h for h in hashtags if h in ALLOWED_HASHTAGS_SET]
            text = re.sub(r"#\w+", "", text).strip()
            if allowed_hashtags:
                text = text + "\n\n" + " ".join(allowed_hashtags)
            data["formatted_text"] = text

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

    async def generate_alert(self, event: dict, minutes_left: int, motivational_index: int = 0) -> str:
        impact_emoji = "🔴" if event.get("impact") == "red" else "🟠"
        event_name = event.get("name", "Unknown Event")
        motivational_line = _get_motivational_line(event_name, motivational_index)
        prompt = _ALERT_PROMPT_TEMPLATE.format(
            event_name=event_name,
            time_12h=event.get("time_12h", "—"),
            impact_emoji=impact_emoji,
            minutes_left=minutes_left,
            motivational_line=motivational_line,
        )
        try:
            result = await asyncio.wait_for(self._gemini_text_call(prompt), timeout=30)
            log.info(f"Alert generated for: {event_name} ({minutes_left} min)")
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
            return self._fallback_alert(event, minutes_left, motivational_index)

    async def get_motivational_line(self, event_name: str, fallback_index: int = 0) -> str:
        """Public wrapper for _get_motivational_line."""
        return _get_motivational_line(event_name, fallback_index)

    def _build_moderation_prompt(self, text: str) -> str:
        return textwrap.dedent(f"""
            DATE (UTC): {_today_str()}
            CHANNEL FOCUS: {self._category}
            SOURCE CONTENT:
            \"\"\"
            {text.strip() if text else "(image only — no text)"}
            \"\"\"
            TASK: Analyse content. If it is relevant geopolitical/macro news (Gold, Oil, USD, central banks, geopolitics, energy, political leaders) OR actual released economic data (with numbers like "2.5% came", "rose to 2.5%", "was 2.5%"), then approve and format.
            If it contains forecast or previous values (e.g., "forecast 2.5%", "previous 2.3%"), reject.
            If it is signal, TA, meme, off‑topic, low‑value, stale – reject.
            Format according to rules. Return JSON.
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
    def _fallback_alert(event: dict, minutes_left: int, motivational_index: int = 0) -> str:
        emoji = "🔴" if event.get("impact") == "red" else "🟠"
        event_name = event.get("name", "Unknown Event")
        line = _get_motivational_line(event_name, motivational_index)
        text = (
            f"🚨 ALERT: {minutes_left} MINUTES REMAINING\n\n"
            f"{emoji} {event_name}\n"
            f"🕒 {event.get('time_12h', '—')}\n\n"
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
    lines = text.split('\n')
    for i in range(max(0, len(lines)-3), len(lines)):
        lines[i] = re.sub(r'\b\d{4}\b', '', lines[i])
    text = '\n'.join(lines)
    text = re.sub(r'\n\s*\n', '\n\n', text).strip()
    text = _add_signature(text)
    return text
