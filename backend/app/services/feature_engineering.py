from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping


IST = timezone(timedelta(hours=5, minutes=30))


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_dt(s: str) -> datetime:
    # Stored as ISO8601 with timezone in generate_data.py
    return datetime.fromisoformat(s)


def _sum(rows: Iterable[Mapping[str, Any]], key: str) -> float:
    return float(sum(float(r[key]) for r in rows))


def _safe_div(n: float, d: float) -> float:
    return float(n / d) if d else 0.0


def _pct_change(curr: float, prev: float) -> float:
    if prev == 0:
        return 0.0 if curr == 0 else 1.0
    return float((curr - prev) / prev)


@dataclass(frozen=True)
class Windows:
    today: tuple[datetime, datetime]
    last_7: tuple[datetime, datetime]
    prev_7: tuple[datetime, datetime]
    last_30: tuple[datetime, datetime]
    last_90: tuple[datetime, datetime]
    prev_45: tuple[datetime, datetime]
    last_45: tuple[datetime, datetime]


def _make_windows(as_of: datetime) -> Windows:
    # Normalize to IST day boundaries.
    as_of = as_of.astimezone(IST)
    day_start = datetime(as_of.year, as_of.month, as_of.day, tzinfo=IST)
    day_end = day_start + timedelta(days=1)

    last_7_start = day_start - timedelta(days=6)  # inclusive of today
    prev_7_start = last_7_start - timedelta(days=7)

    last_30_start = day_start - timedelta(days=29)
    last_90_start = day_start - timedelta(days=89)

    # QoQ proxy windows. If you only have ~90 days, use two 45-day blocks.
    last_45_start = day_start - timedelta(days=44)
    prev_45_start = last_45_start - timedelta(days=45)

    return Windows(
        today=(day_start, day_end),
        last_7=(last_7_start, day_end),
        prev_7=(prev_7_start, last_7_start),
        last_30=(last_30_start, day_end),
        last_90=(last_90_start, day_end),
        prev_45=(prev_45_start, last_45_start),
        last_45=(last_45_start, day_end),
    )


def _fetch_success(conn: sqlite3.Connection, merchant_id: str, start: datetime, end: datetime) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT amount, payer_hash, is_recharge, created_at
            FROM transactions
            WHERE merchant_id = ?
              AND status = 'SUCCESS'
              AND created_at >= ?
              AND created_at < ?
            """,
            (merchant_id, start.isoformat(), end.isoformat()),
        )
    )


def _fetch_all(conn: sqlite3.Connection, merchant_id: str, start: datetime, end: datetime) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT amount, payer_hash, is_recharge, status, created_at
            FROM transactions
            WHERE merchant_id = ?
              AND created_at >= ?
              AND created_at < ?
            """,
            (merchant_id, start.isoformat(), end.isoformat()),
        )
    )


def _group_by_day(rows: Iterable[sqlite3.Row]) -> dict[date, list[sqlite3.Row]]:
    out: dict[date, list[sqlite3.Row]] = {}
    for r in rows:
        dt = _parse_dt(r["created_at"]).astimezone(IST)
        out.setdefault(dt.date(), []).append(r)
    return out


def _group_by_hour(rows: Iterable[sqlite3.Row]) -> dict[int, list[sqlite3.Row]]:
    out: dict[int, list[sqlite3.Row]] = {}
    for r in rows:
        dt = _parse_dt(r["created_at"]).astimezone(IST)
        out.setdefault(dt.hour, []).append(r)
    return out


def compute_merchant_features(
    *,
    transactions_db_path: str,
    merchant_id: str,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """
    Compute merchant-level features + context/alerts.

    Returns a structured dict only (no logging/printing).
    """
    as_of = (as_of or datetime.now(tz=IST)).astimezone(IST)
    w = _make_windows(as_of)

    conn = _connect(transactions_db_path)
    try:
        # Pull once for each window we need.
        succ_30 = _fetch_success(conn, merchant_id, *w.last_30)
        succ_7 = _fetch_success(conn, merchant_id, *w.last_7)
        succ_prev7 = _fetch_success(conn, merchant_id, *w.prev_7)
        succ_today = _fetch_success(conn, merchant_id, *w.today)
        succ_45 = _fetch_success(conn, merchant_id, *w.last_45)
        succ_prev45 = _fetch_success(conn, merchant_id, *w.prev_45)

        all_30 = _fetch_all(conn, merchant_id, *w.last_30)

        # daily_activity_rate: active days / 30 days
        days_with_tx = len(_group_by_day(succ_30))
        daily_activity_rate = _safe_div(days_with_tx, 30.0)

        # revenue_growth_qoq: QoQ proxy using two 45-day blocks
        revenue_last45 = _sum(succ_45, "amount")
        revenue_prev45 = _sum(succ_prev45, "amount")
        revenue_growth_qoq = _pct_change(revenue_last45, revenue_prev45)

        # avg_ticket_trend: compare avg ticket last 7 vs previous 7
        avg_ticket_7 = _safe_div(_sum(succ_7, "amount"), float(len(succ_7)))
        avg_ticket_prev7 = _safe_div(_sum(succ_prev7, "amount"), float(len(succ_prev7)))
        avg_ticket_trend = _pct_change(avg_ticket_7, avg_ticket_prev7)

        # peak_hour_consistency: frequency of the most common peak hour (by revenue) over active days
        day_groups = _group_by_day(succ_30)
        peak_hours: list[int] = []
        for day_rows in day_groups.values():
            by_hour = _group_by_hour(day_rows)
            hour_revenues = {h: _sum(rs, "amount") for h, rs in by_hour.items()}
            peak_hour = max(hour_revenues.items(), key=lambda kv: kv[1])[0] if hour_revenues else 0
            peak_hours.append(int(peak_hour))
        if peak_hours:
            freq: dict[int, int] = {}
            for h in peak_hours:
                freq[h] = freq.get(h, 0) + 1
            peak_hour_consistency = _safe_div(max(freq.values()), float(len(peak_hours)))
        else:
            peak_hour_consistency = 0.0

        # repeat_customer_ratio: share of tx coming from repeat payers (last 30)
        payer_counts: dict[str, int] = {}
        for r in succ_30:
            payer = str(r["payer_hash"])
            payer_counts[payer] = payer_counts.get(payer, 0) + 1
        repeat_tx = sum(c for c in payer_counts.values() if c >= 2)
        repeat_customer_ratio = _safe_div(float(repeat_tx), float(len(succ_30)))

        # recharge_goods_ratio: recharge amount / goods amount (last 30)
        recharge_amt = float(sum(float(r["amount"]) for r in succ_30 if int(r["is_recharge"]) == 1))
        goods_amt = float(sum(float(r["amount"]) for r in succ_30 if int(r["is_recharge"]) == 0))
        recharge_goods_ratio = _safe_div(recharge_amt, goods_amt)

        # Context
        today_revenue = _sum(succ_today, "amount")
        revenue_7 = _sum(succ_7, "amount")
        revenue_prev7 = _sum(succ_prev7, "amount")
        wow_delta = _pct_change(revenue_7, revenue_prev7)

        tx_7 = len(succ_7)
        tx_prev7 = len(succ_prev7)
        velocity_drop = float(max(0.0, -_pct_change(float(tx_7), float(tx_prev7))))
        ticket_drop = float(max(0.0, -_pct_change(avg_ticket_7, avg_ticket_prev7)))

        # Failure rate alert (last 30, all statuses)
        failed_30 = sum(1 for r in all_30 if str(r["status"]) == "FAILED")
        fail_rate_30 = _safe_div(float(failed_30), float(len(all_30)))

        alerts: list[dict[str, Any]] = []
        if wow_delta <= -0.20:
            alerts.append({"type": "REVENUE_DROP_WOW", "severity": "high", "value": wow_delta})
        elif wow_delta <= -0.10:
            alerts.append({"type": "REVENUE_DROP_WOW", "severity": "medium", "value": wow_delta})

        if velocity_drop >= 0.20:
            alerts.append({"type": "VELOCITY_DROP", "severity": "high", "value": velocity_drop})
        elif velocity_drop >= 0.10:
            alerts.append({"type": "VELOCITY_DROP", "severity": "medium", "value": velocity_drop})

        if ticket_drop >= 0.15:
            alerts.append({"type": "TICKET_DROP", "severity": "medium", "value": ticket_drop})

        if fail_rate_30 >= 0.20:
            alerts.append({"type": "HIGH_FAILURE_RATE", "severity": "high", "value": fail_rate_30})

        # Recharge dominating could indicate "recharge shop" behavior.
        recharge_share = _safe_div(recharge_amt, recharge_amt + goods_amt)
        if recharge_share >= 0.60 and (recharge_amt + goods_amt) > 0:
            alerts.append({"type": "HIGH_RECHARGE_SHARE", "severity": "info", "value": recharge_share})

        return {
            "features": {
                "daily_activity_rate": daily_activity_rate,
                "revenue_growth_qoq": revenue_growth_qoq,
                "avg_ticket_trend": avg_ticket_trend,
                "peak_hour_consistency": peak_hour_consistency,
                "repeat_customer_ratio": repeat_customer_ratio,
                "recharge_goods_ratio": recharge_goods_ratio,
            },
            "context": {
                "today_revenue": today_revenue,
                "wow_delta": wow_delta,
                "velocity_drop": velocity_drop,
                "ticket_drop": ticket_drop,
                "alerts": alerts,
            },
            "as_of": as_of.isoformat(),
            "merchant_id": merchant_id,
        }
    finally:
        conn.close()

