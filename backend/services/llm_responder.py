from __future__ import annotations

import json
import os
import urllib.request
from typing import Any


SYSTEM_PROMPT = (
    "You are Dukaan Dost, an AI business advisor built into the Paytm Soundbox for Indian kirana merchants. "
    "You speak Hindi. Your responses are maximum 3 short sentences. You are direct, specific, and actionable. "
    "You only use the data provided to you — never make up numbers. If a merchant asks about their business, "
    "give them concrete advice based on their actual data. Speak like a trusted friend who understands their "
    "business deeply, not like a customer service bot."
)


def _build_user_prompt(*, context: dict[str, Any], intent: str, transcript: str) -> str:
    context_json = json.dumps(context, ensure_ascii=False)
    return (
        "Merchant context (use these exact numbers in your response):\n"
        f"{context_json}\n\n"
        f"Merchant's intent: {intent}\n"
        f'Merchant said: "{transcript}"\n\n'
        "Respond in Hindi in maximum 3 sentences. Be specific — use the actual numbers from the context."
    )


def groq_hindi_response(*, context: dict[str, Any], intent: str, transcript: str) -> str:
    """
    Call Groq API with structured context and return Hindi response text.

    Uses:
    - model: llama-3.1-70b-versatile
    - max_tokens: 250
    - temperature: 0.4
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        # Keep the API layer predictable in local dev without secrets.
        return "Abhi main jawab generate nahi kar pa raha hoon. Kripya thodi der baad try karein."

    url = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1/chat/completions")
    model = "llama-3.1-70b-versatile"

    payload = {
        "model": model,
        "temperature": 0.4,
        "max_tokens": 250,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": _build_user_prompt(context=context, intent=intent, transcript=transcript),
            },
        ],
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        data = json.loads(body)
        content = data["choices"][0]["message"]["content"]
        if not isinstance(content, str):
            return "Abhi main jawab generate nahi kar pa raha hoon. Kripya thodi der baad try karein."
        return content.strip()
    except Exception:
        return "Abhi main jawab generate nahi kar pa raha hoon. Kripya thodi der baad try karein."

