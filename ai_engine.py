"""
ai_engine.py — Dual-layer AI analysis engine.

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

_SYSTEM_PROMPT = """
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


class AIEngine:
    def __init__(self, gemini_key: str, groq_key: str, channel_category: str):
        self._category = channel_category
        self._groq = AsyncGroq(api_key=groq_key)

        genai.configure(api_key=gemini_key)
        self._gemini = genai.GenerativeModel(
            model_name="gemini-2.5-flash-preview-05-20",
            system_instruction=_SYSTEM_PROMPT,
            generation_config=genai.GenerationConfig(
                temperature=0.15,
                max_output_tokens=350,
                response_mime_type="application/json",
            ),
        )

    async def analyse(
        self,
        text: str,
        image_data: Optional[bytes] = None,
        image_mime: str = "image/jpeg",
    ) -> dict:

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

        prompt = self._build_prompt(text)

        # Layer 1 — Gemini 2.5 Flash
        try:
            verdict = await asyncio.wait_for(
                self._gemini_call(prompt, image_data, image_mime), timeout=35
            )
            verdict["engine"] = "gemini-2.5-flash"
            log.info(f"Gemini → approved={verdict['approved']} | {verdict['reason']}")
            return verdict
        except Exception as exc:
            log.warning(f"Gemini failed ({exc}) — trying Groq …")

        # Layer 2 — Groq Vision fallback
        try:
            verdict = await asyncio.wait_for(
                self._groq_call(prompt, image_data, image_mime), timeout=50
            )
            verdict["engine"] = "groq-llama4-vision"
            log.info(f"Groq → approved={verdict['approved']} | {verdict['reason']}")
            return verdict
        except Exception as exc:
            log.error(f"Groq failed ({exc}) — safe reject.")
            return {
                "approved": False,
                "reason": "Both engines unavailable.",
                "issues": ["engine_error"],
                "formatted_text": "",
                "hashtags": "",
                "confidence": 0.0,
                "engine": "none",
            }

    def _build_prompt(self, text: str) -> str:
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

    async def _gemini_call(self, prompt, image_data, image_mime):
        parts = []
        if image_data:
            parts.append({"inline_data": {"mime_type": image_mime, "data": _b64(image_data)}})
        parts.append(prompt)
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(None, lambda: self._gemini.generate_content(parts))
        return _parse_json(resp.text)

    async def _groq_call(self, prompt, image_data, image_mime):
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
            max_tokens=350,
        )
        return _parse_json(resp.choices[0].message.content)
