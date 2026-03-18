from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class IntentResult:
    intent: str
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        return {"intent": self.intent, "confidence": float(self.confidence)}


KEYWORD_INTENTS: list[tuple[str, str]] = [
    ("CFO", "CFO"),
    ("CREDIT", "CREDIT"),
    ("COO", "COO"),
    ("FRAUD", "FRAUD"),
    ("SUPPORT", "SUPPORT"),
]


def _mock_llm_fallback(_text: str) -> IntentResult:
    # No real LLM yet. Keep stable and predictable for demos/tests.
    return IntentResult(intent="GENERAL", confidence=0.55)


def route_intent(text: str) -> dict[str, Any]:
    """
    Keyword-first intent routing.

    Returns a structured dict only:
    {"intent": "...", "confidence": 0.9}
    """
    normalized = (text or "").strip().upper()

    for keyword, intent in KEYWORD_INTENTS:
        if keyword in normalized:
            return IntentResult(intent=intent, confidence=0.9).to_dict()

    return _mock_llm_fallback(text).to_dict()

