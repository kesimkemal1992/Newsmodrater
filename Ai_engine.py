"""
ai_engine.py — Dual-layer AI analysis engine.

Layer 1 (Primary)  : Google Gemini 2.5 Flash  — text + vision
Layer 2 (Fallback) : Groq (llama-3.2-11b-vision-preview) — text + vision

Responsibilities:
  • Relevance check — is content within channel category?
  • Watermark / logo detection on images
  • Date/time validation — content must be TODAY
  • Professional Amharic-English formatting
  • Hashtag generation (only affected trading pairs)
"""

import asyncio
import base64
import json
import logging
import re
import textwrap
from datetime import datetime, timezone
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
#   "issues": list[str],         # ["watermark", "off_topic", "old_content"]
# }

_SYSTEM_PROMPT = """
You are AXIOM — a World-Class Market Analyst and Editorial AI for a premium \
Forex & Commodities intelligence Telegram channel.

YOUR ROLE:
You screen, analyse, and reformat financial market content with the \
discipline of a Bloomberg editor and the insight of a 20-year veteran trader.

STRICT QUALITY GATES (reject if ANY apply):
1. WATERMARK / LOGO  — Image contains another channel's watermark, logo, \
   username, or brand. → REJECT (keeps our channel professional).
2. OFF-TOPIC CONTENT — Content is NOT related to: Forex pairs, Gold (XAU), \
   Silver, Oil (WTI/Brent), indices (US30, NAS100, SPX500), crypto majors \
   (BTC/ETH), or macro economic events that directly move markets. \
   Memes, politics unrelated to markets, celebrity news → REJECT.
3. STALE CONTENT     — Charts, screenshots, or news dated MORE THAN 18 \
   hours ago. Always look for timestamps in images or text. If clearly old → REJECT.
4. DUPLICATE / LOW-VALUE — Generic "market is moving" posts with no \
   analysis, no specific levels, no insight → REJECT.
5. UNVERIFIABLE DATA — Wild claims without source, unrealistic pip targets → REJECT.

IF APPROVED — FORMATTING RULES:
• Write in professional Urban Amharic-English mix (code-switch naturally). \
  Start with a powerful 1-line hook in Amharic, then body in English with \
  Amharic phrases woven in for emphasis.
• Structure: Hook → Context → Key Levels / Data → Insight / Bias → Caution note
• Use clean Telegram-compatible formatting: *bold* for pair names and key \
  numbers, no HTML tags.
• Footer: ONLY hashtags of the specific instruments mentioned. \
  Format: #XAUUSD #USDJPY #USD — NO generic hashtags like #forex or #trading.
• Length: 150–280 words. Concise. Institutional. Zero fluff.
• Do NOT copy-paste the source text. Rewrite with analytical value added.

RESPONSE FORMAT — respond ONLY with valid JSON, no markdown fences, no preamble:
{
  "approved": true | false,
  "reason": "brief reason for decision",
  "issues": ["watermark" | "off_topic" | "old_content" | "duplicate" | \
              "low_value" | "unverifiable"],
  "formatted_text": "full formatted post (only if approved, else empty string)",
  "hashtags": "#PAIR1 #PAIR2 (only if approved, else empty string)",
  "confidence": 0.0
}
""".strip()


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d (UTC)")


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
                temperature=0.3,
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
        """
        Returns a verdict dict.  Tries Gemini first; falls back to Groq.
        """
        user_prompt = self._build_user_prompt(text)

        # --- Layer 1: Gemini 2.5 Flash ---
        try:
            verdict = await asyncio.wait_for(
                self._gemini_analyse(user_prompt, image_data, image_mime),
                timeout=30,
            )
            verdict["engine"] = "gemini"
            log.info(
                f"Gemini verdict: approved={verdict.get('approved')} "
                f"reason={verdict.get('reason')}"
            )
            return verdict
        except Exception as exc:
            log.warning(f"Gemini failed ({exc}), falling back to Groq …")

        # --- Layer 2: Groq (llama-3.2-vision) ---
        try:
            verdict = await asyncio.wait_for(
                self._groq_analyse(user_prompt, image_data, image_mime),
                timeout=40,
            )
            verdict["engine"] = "groq"
            log.info(
                f"Groq verdict: approved={verdict.get('approved')} "
                f"reason={verdict.get('reason')}"
            )
            return verdict
        except Exception as exc:
            log.error(f"Groq also failed: {exc}")
            return {
                "approved": False,
                "reason": "Both AI engines failed — skipping for safety.",
                "issues": ["engine_error"],
                "formatted_text": "",
                "hashtags": "",
                "confidence": 0.0,
                "engine": "none",
            }

    # ── Prompt builder ─────────────────────────────────────────────────────────
    def _build_user_prompt(self, text: str) -> str:
        return textwrap.dedent(f"""
            TODAY'S DATE (UTC): {_today_str()}
            CHANNEL CATEGORY: {self._category}

            SOURCE CONTENT:
            \"\"\"
            {text or "(no text — image only)"}
            \"\"\"

            Instructions:
            1. If an image is attached, analyse it carefully for watermarks, \
logos, timestamps, and chart data.
            2. Apply all QUALITY GATES.
            3. If approved, produce the professionally formatted post.
            4. Return ONLY the JSON object described in your system prompt.
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

    # ── Groq call ─────────────────────────────────────────────────────────────
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
                "image_url": {
                    "url": f"data:{image_mime};base64,{_b64(image_data)}"
                },
            })

        messages_content.append({"type": "text", "text": prompt})

        response = await self._groq.chat.completions.create(
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": messages_content},
            ],
            temperature=0.25,
            max_tokens=1024,
        )
        raw = response.choices[0].message.content
        return self._parse_json(raw)

    # ── JSON parser (robust) ───────────────────────────────────────────────────
    @staticmethod
    def _parse_json(raw: str) -> dict:
        # Strip markdown fences if model ignores mime_type instruction
        raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            # Try to extract first JSON object with regex
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            if m:
                data = json.loads(m.group())
            else:
                raise ValueError(f"No JSON found in AI response: {raw[:200]}")

        # Normalise / fill defaults
        data.setdefault("approved", False)
        data.setdefault("reason", "")
        data.setdefault("issues", [])
        data.setdefault("formatted_text", "")
        data.setdefault("hashtags", "")
        data.setdefault("confidence", 0.5)
        return data
