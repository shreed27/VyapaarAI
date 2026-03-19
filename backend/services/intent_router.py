from __future__ import annotations

import json
import os
import re
import urllib.request
from typing import Any


INTENT_KEYWORDS: dict[str, list[str]] = {
    "CFO": [
        "paisa",
        "kamai",
        "revenue",
        "hisaab",
        "settlement",
        "kitna",
        "aya",
        "aaya",
        "income",
        "earning",
        "profit",
        "loss",
        "balance",
    ],
    "CREDIT": ["loan", "credit", "udhar", "paise chahiye", "borrow", "limit", "score", "eligibility"],
    "COO": ["transaction", "bika", "bikta", "slow", "band", "customers", "kitne", "average", "pattern"],
    "FRAUD": ["fraud", "scam", "suspicious", "block", "fake", "dhoka", "collect request", "unknown"],
    "SUPPORT": ["failed", "fail", "nahi aaya", "deduct", "cut", "refund", "wapas", "pending", "stuck", "error"],
}

_INTENT_PRIORITY: list[str] = ["CFO", "CREDIT", "COO", "FRAUD", "SUPPORT"]


def _keyword_classify(transcript: str) -> tuple[str | None, int]:
    t = (transcript or "").lower()
    best_intent: str | None = None
    best_hits = 0

    for intent in _INTENT_PRIORITY:
        hits = 0
        for kw in INTENT_KEYWORDS[intent]:
            if kw in t:
                hits += 1
        if hits > best_hits:
            best_hits = hits
            best_intent = intent
        elif hits == best_hits and hits > 0 and best_intent is not None:
            # Stable tie-break by priority order
            if _INTENT_PRIORITY.index(intent) < _INTENT_PRIORITY.index(best_intent):
                best_intent = intent

    return best_intent, best_hits


def _groq_classify(transcript: str) -> dict[str, Any]:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not set")

    model = os.getenv("GROQ_MODEL", "llama3-8b-8192")
    url = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1/chat/completions")

    prompt = (
        "You are an intent classifier for a merchant voice assistant. "
        "Classify the transcript into exactly one of: CFO, CREDIT, COO, FRAUD, SUPPORT. "
        "Return ONLY valid JSON like: {\"intent\":\"CFO\",\"confidence\":0.7}. "
        "Confidence must be a number between 0 and 1.\n\n"
        f"Transcript: {transcript}"
    )

    payload = {
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": "Return JSON only. No extra text."},
            {"role": "user", "content": prompt},
        ],
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8", errors="replace")

    data = json.loads(body)
    content = data["choices"][0]["message"]["content"]
    parsed = _parse_intent_json(content)
    if parsed is None:
        raise ValueError("Could not parse Groq response as intent JSON")
    return parsed


def _parse_intent_json(text: str) -> dict[str, Any] | None:
    # Try direct JSON first.
    try:
        obj = json.loads(text)
    except Exception:
        # Attempt to extract a JSON object from a messy response.
        m = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
        except Exception:
            return None

    if not isinstance(obj, dict):
        return None
    intent = obj.get("intent")
    conf = obj.get("confidence")
    if intent not in INTENT_KEYWORDS:
        return None
    try:
        conf_f = float(conf)
    except Exception:
        return None
    conf_f = max(0.0, min(1.0, conf_f))
    return {"intent": intent, "confidence": conf_f}


def route_intent(transcript: str) -> dict[str, Any]:
    """
    Classify Hindi/English transcript into one of: CFO, CREDIT, COO, FRAUD, SUPPORT.

    Keyword match first:
    - If any keyword matches, return intent with confidence 0.9
    - If multiple match, return the one with the most hits
    - If none match, call Groq as fallback and parse JSON response

    Returns structured dict only: {"intent": "...", "confidence": 0.9}
    """
    intent, hits = _keyword_classify(transcript)
    if intent and hits > 0:
        return {"intent": intent, "confidence": 0.9}

    try:
        result = _groq_classify(transcript)
        # Ensure return contract
        return {"intent": result["intent"], "confidence": float(result["confidence"])}
    except Exception:
        # Safe deterministic fallback if Groq is unavailable.
        return {"intent": "SUPPORT", "confidence": 0.55}

