from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field

from backend.app.services.context_builder import build_context
from backend.app.services.engines import run_engine
from backend.app.services.intent_router import route_intent
from backend.app.services.stt_mock import mock_stt

router = APIRouter()


class VoiceRequest(BaseModel):
    merchant_id: str = Field(..., description="Merchant identifier")
    audio_b64: str | None = Field(None, description="Base64-encoded audio (mocked for now)")
    locale: str = Field("hi-IN", description="Locale, e.g. hi-IN")


@router.post("")
def voice_endpoint(req: VoiceRequest) -> dict:
    text_hi = mock_stt(req.audio_b64)
    intent_result = route_intent(text_hi)
    ctx = build_context(merchant_id=req.merchant_id, locale=req.locale)
    engine_result = run_engine(intent=intent_result["intent"], text_hi=text_hi, context=ctx)

    return {
        "input": {"merchant_id": req.merchant_id, "locale": req.locale},
        "stt": {"text_hi": text_hi},
        "intent": intent_result,
        "context": ctx,
        "result": engine_result,
    }
