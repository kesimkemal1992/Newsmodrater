"""
ai_engine.py — Dual-layer AI analysis engine.
Trump/market-moving detection without extra hashtags.
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

# ─── Only three allowed hashtags ────────────────────────────────────────────
ALLOWED_HASHTAGS = "#XAUUSD #DXY #OIL"

# ─── Trump / market-moving keywords (for prioritisation) ────────────────────
TRUMP_KEYWORDS = [
    "trump", "donald trump", "president trump", "trump's", "trump tariff",
    "trump speech", "trump rally", "trump announcement", "trump media",
    "former president trump", "trump investigation", "trump indictment"
]
MARKET_MOVING_KEYWORDS = [
    "market moving", "market effect", "volatility alert", "high impact",
    "risk event", "flash crash", "liquidity flush"
]

def _is_trump_or_market_moving(text: str) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    for kw in TRUMP_KEYWORDS:
        if kw in text_lower:
            return True
    for kw in MARKET_MOVING_KEYWORDS:
        if kw in text_lower:
            return True
    return False

# ─── System prompt (no extra hashtags, no #GEOPOLITICS) ─────────────────────
_SYSTEM_PROMPT = """
You are AXIOM INTEL — a Senior Institutional Macro & Geopolitical news editor
for a professional Telegram trading channel. Your audience is experienced traders.

SPECIAL INSTRUCTION FOR TRUMP / MARKET-MOVING NEWS:
- News about Donald Trump, his policies, speeches, or legal developments that could move
  Gold, Oil, or USD must be treated with HIGH PRIORITY.
- Always APPROVE such news if it is factual and recent (<18h).
- Use the 🗳️ emoji for political events that affect markets.
- Hashtags: ONLY #XAUUSD #DXY #OIL — no other hashtags ever.

YOUR ONLY JOB:
Take the source content, verify its relevance, and format it cleanly.
Do NOT speculate. Do NOT add analysis beyond the facts.
Do NOT change the meaning. Format it professionally and precisely.

CRITICAL FORMATTING RULES:
- DO NOT use asterisks (*) or any markdown bolding anywhere
- Use ONLY plain text and emojis
- NO NOTE line. NO MARKET STATUS line. NO commentary line at the end.
- NO forecast, NO previous data — never include numbers or data tables
- Hashtags: ONLY #XAUUSD #DXY #OIL — no other hashtags ever
- Do NOT add signature — it is added automatically after

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REJECT IF ANY OF THESE APPLY:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. SIGNALS       — Buy / Sell / Long / Short / Entry / TP / SL / price targets
2. CHART / TA    — Technical analysis, chart patterns, indicators, setups
3. MEME          — Meme images, jokes, informal content, opinion posts
4. ANALYSIS IMG  — Chart screenshots, TA images, trade idea images
5. WATERMARK     — Another channel logo or username visible on image
6. STALE         — Content older than 18 hours
7. OFF-TOPIC     — Not about geopolitics, central banks, macro data,
                   Gold, Oil, USD — strictly no other topics
8. LOW VALUE     — Vague, no specific real-world event
9. DUPLICATE     — Same story already processed (even if worded differently)
10. PREDICTION   — "I think", "expect", "my analysis", "in my opinion"
11. COMMENTARY   — Personal views, market opinions, trade recommendations

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FORMAT (if approved — geopolitical/macro news only):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[EMOJI] [SHORT ENGLISH HEADLINE — one line, factual and direct]

[Source content lightly cleaned. English only. 2-4 sentences max.
Plain facts only. No bold. No asterisks. Do not add anything new.]

#XAUUSD #DXY #OIL

EMOJI — pick ONE that matches the story:
  🚨 Breaking news     🌍 Geopolitics / war / sanctions
  📊 Economic data     🏦 Central bank decision
  🛢️ Oil/energy        🏆 Gold/commodities
  💵 USD/FX flows      ⚠️ Risk event / crisis
  🗳️ Political leader / Trump / major election event

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPOND WITH VALID JSON ONLY — NO MARKDOWN FENCES — NO TRAILING COMMAS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{"approved": true, "reason": "brief reason", "issues": [], "formatted_text": "post text here without signature", "confidence": 0.9}
""".strip()

# ─── Similarity check (unchanged) ───────────────────────────────────────────
_SIMILARITY_PROMPT = """
You are a duplicate news detector.
Compare these two news stories and decide if they are about the SAME real-world event.
Ignore differences in wording, language, or phrasing — only judge the underlying story.

Story A:
{story_a}

Story B:
{story_b}

Are these the same story? Respond with JSON only:
{"same_story": true, "confidence": 0.95, "reason": "brief reason"}
""".strip()

# ─── ForexFactory image prompts (unchanged) ──────────────────────────────────
_FF_IMAGE_PROMPT = """
You are analysing a ForexFactory economic calendar screenshot posted in a Telegram channel.

TODAY'S DATE: {today_date}

YOUR TASKS:
1. Confirm this is a real ForexFactory calendar image (not a meme, chart, or other image)
2. Confirm it shows TODAY's date ({today_date}) — reject if it shows another date
3. Extract only USD high-impact (Red) and medium-impact (Orange) events visible
4. Format a clean daily briefing — NO forecast, NO previous data

STRICT RULES:
- Only USD events
- Only Red (🔴) and Orange (🟠) impact
- Times in 12-hour AM/PM format only (no timezone label)
- NO forecast values, NO previous values
- NO NOTE line, NO commentary
- Plain text only, no asterisks, no bold
- Do NOT add signature

If this is NOT a ForexFactory calendar image, or NOT today's date, respond with:
{{"approved": false, "reason": "not a valid ForexFactory today image"}}

If valid, respond with:
{{"approved": true, "reason": "valid FF today image", "formatted_text": "📅 TODAY'S USD HIGH IMPACT NEWS\\nDay, Month DD, YYYY\\n\\n🔴 03:30 PM | USD: Event Name\\n🟠 05:00 PM | USD: Another Event\\n\\nBe careful during these releases."}}

RESPOND WITH VALID JSON ONLY — NO MARKDOWN FENCES — NO TRAILING COMMAS.
""".strip()

_FF_WEEKLY_IMAGE_PROMPT = """
You are analysing a ForexFactory economic calendar screenshot for the weekly outlook.

CURRENT WEEK: {week_range}

YOUR TASKS:
1. Confirm this is a real ForexFactory calendar image
2. Extract USD high-impact (Red) and medium-impact (Orange) events for this week
3. Format a clean weekly calendar — NO forecast, NO previous data

STRICT RULES:
- Only USD events
- Only Red (🔴) and Orange (🟠) impact
- Times in 12-hour AM/PM format only (no timezone label)
- Group by day
- NO forecast values, NO previous values
- NO NOTE line, NO commentary
- Plain text only, no asterisks, no bold
- Do NOT add signature

If NOT a valid ForexFactory image, respond with:
{{"approved": false, "reason": "not a valid ForexFactory calendar image"}}

If valid, respond with JSON containing formatted_text like:
{{"approved": true, "reason": "valid FF weekly image", "formatted_text": "📅 WEEKLY HIGH IMPACT NEWS\\nWeek of {week_range}\\n\\nMonday — Apr 28\\n🔴 03:30 PM | USD: Event Name\\n\\nFriday — May 02\\n🔴 03:30 PM | USD: NFP"}}

RESPOND WITH VALID JSON ONLY — NO MARKDOWN FENCES — NO TRAILING COMMAS.
""".strip()

# ─── 10-Minute Alert (unchanged) ────────────────────────────────────────────
_ALERT_PROMPT_TEMPLATE = """
You are a Senior Institutional Trader writing a pre-event warning alert.

STRICT RULES:
- DO NOT use asterisks (*) or any markdown bolding — plain text only
- English only
- Say NEWS: not EVENT:
- Do NOT include Forecast or Previous — skip those lines entirely
- Do NOT add NOTE or commentary
- Do NOT add signature
- Copy the motivational closing line exactly as given — do not modify it

News details:
Name: {event_name}
Currency: {currency}
Time: {time_12h}
Impact: {impact_emoji}

Motivational closing line (copy exactly):
{motivational_line}

Write in this EXACT format:

🚨 ALERT: 10 MINUTES REMAINING

NEWS: {impact_emoji} {event_name}
TIME: {time_12h}

REQUIRED ACTION:
✅ Secure open profits now
✅ Move Stop-Loss to Break-even
✅ No new entries during the release

{motivational_line}

Return ONLY the formatted alert. No JSON. No markdown. No asterisks.
""".strip()

def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()

def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def _add_signature(text: str) -> str:
    text = text.strip()
    if "[Squad 4xx]" not in text:
        text += CHANNEL_SIGNATURE
    return text

# ─── Motivational pool (unchanged) ──────────────────────────────────────────
_MOTIVATIONAL_POOL = [
    "🛡️ Guard your account like it is your last one — because one day, it might be. Stay safe. 🔒",
    "💰 Your account is everything. One reckless trade during news can erase weeks of hard work. 🚫",
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

_SIGNAL_RE = re.compile(
    r"\b(buy|sell|long|short|entry|tp|take[\s_-]?profit|sl|stop[\s_-]?loss|"
    r"stoploss|stop\s+at\s+\d|entry\s*[:\-]?\s*\d|target\s*[:\-]?\s*\d)\b",
    re.IGNORECASE,
)

_REJECT_PATTERNS = re.compile(
    r"\b(setup|trade idea|my plan|in my opinion|i think|i expect|prediction|"
    r"analysis|meme|chart pattern|support|resistance|trendline|fibonacci|"
    r"elliott wave|ichimoku|rsi|macd|bollinger|moving average)\b",
    re.IGNORECASE,
)

def _signal_hit(text: str) -> Optional[str]:
    if not text:
        return None
    m = _SIGNAL_RE.search(text)
    return m.group(0).strip() if m else None

def _reject_pattern_hit(text: str) -> Optional[str]:
    if not text:
        return None
    m = _REJECT_PATTERNS.search(text)
    return m.group(0).strip() if m else None

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
        data["formatted_text"] = re.sub(
            r"📌\s*(NOTE|MARKET STATUS|STATUS)[^\n]*\n?", "", data["formatted_text"]
        ).strip()
        # Remove any hashtags and re-add only allowed ones
        if data.get("approved"):
            text = data["formatted_text"]
            text = re.sub(r"#\w+", "", text).strip()
            data["formatted_text"] = text

    if data.get("approved") and _signal_hit(data.get("formatted_text", "")):
        log.warning("Signal keyword in output — hard reject.")
        data["approved"] = False
        data["reason"] = "Signal keyword found in output."
        data["issues"].append("signal_content")
        data["formatted_text"] = ""

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
                max_output_tokens=600,
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
        self._gemini_vision = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config=genai.GenerationConfig(
                temperature=0.1,
                max_output_tokens=800,
                response_mime_type="application/json",
            ),
        )

    # ─── News moderation with Trump override ──────────────────────────────────
    async def analyse(
        self,
        text: str,
        image_data: Optional[bytes] = None,
        image_mime: str = "image/jpeg",
    ) -> dict:
        # Trump / market-moving news bypass pre-filters
        if _is_trump_or_market_moving(text):
            log.info("[TRUMP/MARKET] Detected — bypassing standard pre-filters")
            # but still log if signal found (no early reject)
            hit = _signal_hit(text)
            if hit:
                log.warning(f"Signal keyword '{hit}' found but Trump news overrides — AI will decide.")

        if not _is_trump_or_market_moving(text):
            hit = _signal_hit(text)
            if hit:
                log.info(f"[PRE-FILTER] Signal '{hit}' — instant reject.")
                return _reject("Signal keyword detected.", "signal_content")
            pat = _reject_pattern_hit(text)
            if pat:
                log.info(f"[PRE-FILTER] Reject pattern '{pat}' — instant reject.")
                return _reject(f"TA/meme/opinion pattern: '{pat}'", "rejected_pattern")

        prompt = self._build_moderation_prompt(text)

        try:
            verdict = await asyncio.wait_for(
                self._gemini_call(prompt, image_data, image_mime), timeout=40
            )
            verdict["engine"] = "gemini-2.5-flash"
            log.info(f"Gemini → approved={verdict['approved']} | {verdict.get('reason', '')}")
            if verdict.get("approved"):
                verdict["formatted_text"] = _build_post_body(verdict["formatted_text"])
            return verdict
        except Exception as exc:
            log.warning(f"Gemini failed ({exc}) — trying Groq …")

        try:
            verdict = await asyncio.wait_for(
                self._groq_call(prompt, image_data, image_mime), timeout=55
            )
            verdict["engine"] = "groq-llama4-scout"
            log.info(f"Groq → approved={verdict['approved']} | {verdict.get('reason', '')}")
            if verdict.get("approved"):
                verdict["formatted_text"] = _build_post_body(verdict["formatted_text"])
            return verdict
        except Exception as exc:
            log.error(f"Both engines failed — safe reject.")
            return _reject("Both AI engines unavailable.", "engine_error", confidence=0.0)

    async def is_same_story(self, story_a: str, story_b: str) -> bool:
        if not story_a or not story_b:
            return False
        prompt = _SIMILARITY_PROMPT.format(story_a=story_a[:500], story_b=story_b[:500])
        try:
            loop = asyncio.get_event_loop()
            resp = await asyncio.wait_for(
                loop.run_in_executor(None, lambda: self._gemini_vision.generate_content(prompt)), timeout=20
            )
            data = _parse_json(resp.text)
            result = bool(data.get("same_story", False))
            conf = data.get("confidence", 0)
            log.info(f"Similarity check → same_story={result} | confidence={conf}")
            return result and conf >= 0.80
        except Exception as exc:
            log.warning(f"Similarity check failed ({exc}) — assuming not duplicate.")
            return False

    async def analyse_ff_image(self, image_data: bytes, image_mime: str, today_date: str, is_weekly: bool = False, week_range: str = "") -> dict:
        if is_weekly:
            prompt = _FF_WEEKLY_IMAGE_PROMPT.format(week_range=week_range)
        else:
            prompt = _FF_IMAGE_PROMPT.format(today_date=today_date)
        parts = [{"inline_data": {"mime_type": image_mime, "data": _b64(image_data)}}, prompt]
        try:
            loop = asyncio.get_event_loop()
            resp = await asyncio.wait_for(loop.run_in_executor(None, lambda: self._gemini_vision.generate_content(parts)), timeout=45)
            data = _parse_json(resp.text)
            log.info(f"FF image analysis → approved={data.get('approved')} | {data.get('reason', '')}")
            return data
        except Exception as exc:
            log.warning(f"Gemini FF image failed ({exc}) — trying Groq …")
        try:
            content = [{"type": "image_url", "image_url": {"url": f"data:{image_mime};base64,{_b64(image_data)}"}}, {"type": "text", "text": prompt}]
            resp = await asyncio.wait_for(self._groq.chat.completions.create(model="meta-llama/llama-4-scout-17b-16e-instruct", messages=[{"role": "user", "content": content}], temperature=0.1, max_tokens=800), timeout=60)
            data = _parse_json(resp.choices[0].message.content)
            log.info(f"Groq FF image → approved={data.get('approved')}")
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
            return _add_signature(_strip_asterisks(result.strip()))
        except Exception as exc:
            log.warning(f"Gemini alert failed ({exc}) — trying Groq …")
        try:
            result = await asyncio.wait_for(self._groq_text_call(prompt), timeout=45)
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

            TASK:
            1. Check ALL rejection criteria (signals, TA, memes, charts, opinions, duplicates).
            2. If image: check for watermarks, TA charts, memes — reject all of these.
            3. If approved: clean and format. English only. 2-4 sentences max.
               Hashtags: ONLY #XAUUSD #DXY #OIL — no other hashtags.
               NO NOTE line. NO forecast. NO previous. NO asterisks.
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
    return text
