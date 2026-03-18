from __future__ import annotations

from typing import Any


def run_engine(*, intent: str, text_hi: str, context: dict[str, Any]) -> dict[str, Any]:
    """
    Intent-specific handler. For now these are mocked and return structured JSON.
    """
    intent = (intent or "GENERAL").upper()
    if intent == "CFO":
        return _engine_cfo(text_hi=text_hi, context=context)
    if intent == "CREDIT":
        return _engine_credit(text_hi=text_hi, context=context)
    if intent == "COO":
        return _engine_coo(text_hi=text_hi, context=context)
    if intent == "FRAUD":
        return _engine_fraud(text_hi=text_hi, context=context)
    if intent == "SUPPORT":
        return _engine_support(text_hi=text_hi, context=context)
    return _engine_general(text_hi=text_hi, context=context)


def _engine_cfo(*, text_hi: str, context: dict[str, Any]) -> dict[str, Any]:
    return {"engine": "CFO", "action": "SUMMARY", "input_text_hi": text_hi, "context": context}


def _engine_credit(*, text_hi: str, context: dict[str, Any]) -> dict[str, Any]:
    return {"engine": "CREDIT", "action": "CREDIT_STATUS", "input_text_hi": text_hi, "context": context}


def _engine_coo(*, text_hi: str, context: dict[str, Any]) -> dict[str, Any]:
    return {"engine": "COO", "action": "OPS_OVERVIEW", "input_text_hi": text_hi, "context": context}


def _engine_fraud(*, text_hi: str, context: dict[str, Any]) -> dict[str, Any]:
    return {"engine": "FRAUD", "action": "RISK_CHECK", "input_text_hi": text_hi, "context": context}


def _engine_support(*, text_hi: str, context: dict[str, Any]) -> dict[str, Any]:
    return {"engine": "SUPPORT", "action": "TROUBLESHOOT", "input_text_hi": text_hi, "context": context}


def _engine_general(*, text_hi: str, context: dict[str, Any]) -> dict[str, Any]:
    return {"engine": "GENERAL", "action": "ASSIST", "input_text_hi": text_hi, "context": context}

