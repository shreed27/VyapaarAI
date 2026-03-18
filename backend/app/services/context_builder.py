from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any


IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class VoiceContext:
    merchant_id: str
    as_of: str
    locale: str

    def to_dict(self) -> dict[str, Any]:
        return {"merchant_id": self.merchant_id, "as_of": self.as_of, "locale": self.locale}


def build_context(*, merchant_id: str, locale: str = "hi-IN", as_of: datetime | None = None) -> dict[str, Any]:
    """
    Minimal context builder. Later we can attach merchant profile, ledger summaries, etc.
    """
    as_of_dt = (as_of or datetime.now(tz=IST)).astimezone(IST)
    ctx = VoiceContext(merchant_id=merchant_id, as_of=as_of_dt.isoformat(), locale=locale)
    return ctx.to_dict()

