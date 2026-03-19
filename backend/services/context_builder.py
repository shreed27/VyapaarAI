from __future__ import annotations

import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


IST = timezone(timedelta(hours=5, minutes=30))


def _load_dotenv(dotenv_path: str = ".env") -> None:
    """
    Minimal .env loader to avoid adding a dependency.
    Loads KEY=VALUE into os.environ (does not override existing values).
    """
    path = Path(dotenv_path)
    if not path.exists():
        return

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_dt(s: str) -> datetime:
    # Stored as ISO8601 with timezone offset.
    return datetime.fromisoformat(s)


def _day_window(as_of: datetime) -> tuple[datetime, datetime]:
    as_of = as_of.astimezone(IST)
    start = datetime(as_of.year, as_of.month, as_of.day, tzinfo=IST)
    end = start + timedelta(days=1)
    return start, end


def _pct_change(curr: float, prev: float) -> float:
    # Percent change as a percentage number, not a ratio.
    if prev == 0.0:
        return 0.0 if curr == 0.0 else 100.0
    return ((curr - prev) / prev) * 100.0


def _resolve_db_path() -> str:
    # Primary path required by task.
    primary = Path("backend/data/transactions.db")
    if primary.exists():
        return str(primary)

    # Compatibility: earlier scaffolding generated into backend/app/data/.
    fallback = Path("backend/app/data/transactions.db")
    if fallback.exists():
        return str(fallback)

    return str(primary)


def _get_credit_score_default() -> int:
    env_score = os.getenv("CREDIT_SCORE_DEFAULT")
    if env_score:
        try:
            return int(env_score)
        except Exception:
            pass
    return 65


def _load_credit_score(*, merchant_id: str) -> int:
    # No model integration specified; return default unless user provides a value.
    # Supports overriding via env for demos.
    env_fixed = os.getenv("CREDIT_SCORE")
    if env_fixed:
        try:
            return int(env_fixed)
        except Exception:
            return _get_credit_score_default()
    return _get_credit_score_default()


def build_context(*, merchant_id: str, locale: str = "hi-IN", as_of: datetime | None = None) -> dict[str, Any]:
    """
    Build merchant context from transactions.db.

    Returns a structured dict only.
    """
    _load_dotenv()
    _ = locale  # reserved for future localization

    db_path = os.getenv("TRANSACTIONS_DB_PATH") or _resolve_db_path()
    now = (as_of or datetime.now(tz=IST)).astimezone(IST)
    today_start, today_end = _day_window(now)

    last_week_start = today_start - timedelta(days=7)
    last_week_end = today_end - timedelta(days=7)

    last_7_start = today_start - timedelta(days=6)
    last_28_start = today_start - timedelta(days=27)

    today_revenue = 0.0
    today_recharge_revenue = 0.0
    txn_count_today = 0
    avg_ticket_today = 0.0
    avg_ticket_4week = 0.0
    repeat_customers_week = 0
    top_hour = 0

    conn = _connect(db_path)
    try:
        # Today aggregates (SUCCESS only)
        row = conn.execute(
            """
            SELECT
              COALESCE(SUM(amount), 0) AS revenue,
              COALESCE(SUM(CASE WHEN is_recharge = 1 THEN amount ELSE 0 END), 0) AS recharge_revenue,
              COUNT(*) AS txn_count,
              AVG(CASE WHEN is_recharge = 0 THEN amount END) AS avg_ticket_goods
            FROM transactions
            WHERE merchant_id = ?
              AND status = 'SUCCESS'
              AND created_at >= ?
              AND created_at < ?
            """,
            (merchant_id, today_start.isoformat(), today_end.isoformat()),
        ).fetchone()
        if row:
            today_revenue = float(row["revenue"] or 0.0)
            today_recharge_revenue = float(row["recharge_revenue"] or 0.0)
            txn_count_today = int(row["txn_count"] or 0)
            avg_ticket_today = float(row["avg_ticket_goods"] or 0.0)

        today_goods_revenue = float(today_revenue - today_recharge_revenue)

        # WoW delta vs same weekday last week (use revenue)
        last_week_row = conn.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS revenue
            FROM transactions
            WHERE merchant_id = ?
              AND status = 'SUCCESS'
              AND created_at >= ?
              AND created_at < ?
            """,
            (merchant_id, last_week_start.isoformat(), last_week_end.isoformat()),
        ).fetchone()
        last_week_revenue = float(last_week_row["revenue"] or 0.0) if last_week_row else 0.0
        wow_delta_pct = float(_pct_change(today_revenue, last_week_revenue))

        # 7-day average velocity (SUCCESS txns per day)
        rows_7 = list(
            conn.execute(
                """
                SELECT created_at
                FROM transactions
                WHERE merchant_id = ?
                  AND status = 'SUCCESS'
                  AND created_at >= ?
                  AND created_at < ?
                """,
                (merchant_id, last_7_start.isoformat(), today_end.isoformat()),
            )
        )
        counts_by_day: dict[date, int] = {}
        for r in rows_7:
            dt = _parse_dt(str(r["created_at"])).astimezone(IST)
            counts_by_day[dt.date()] = counts_by_day.get(dt.date(), 0) + 1
        last_7_days = [(today_start.date() - timedelta(days=i)) for i in range(6, -1, -1)]
        total_7 = sum(counts_by_day.get(d, 0) for d in last_7_days)
        txn_7day_avg = float(total_7 / 7.0)
        velocity_drop = bool(txn_count_today < txn_7day_avg * 0.6)

        # Avg ticket last 4 weeks excluding recharges (SUCCESS only)
        row_28 = conn.execute(
            """
            SELECT AVG(amount) AS avg_ticket
            FROM transactions
            WHERE merchant_id = ?
              AND status = 'SUCCESS'
              AND is_recharge = 0
              AND created_at >= ?
              AND created_at < ?
            """,
            (merchant_id, last_28_start.isoformat(), today_end.isoformat()),
        ).fetchone()
        avg_ticket_4week = float(row_28["avg_ticket"] or 0.0) if row_28 else 0.0
        ticket_drop = bool(avg_ticket_today < avg_ticket_4week * 0.75) if avg_ticket_4week > 0 else False

        # Repeat customers this week (rolling 7 days): payer_hash count > 1
        rep_row = conn.execute(
            """
            SELECT COUNT(*) AS repeat_payers
            FROM (
              SELECT payer_hash
              FROM transactions
              WHERE merchant_id = ?
                AND status = 'SUCCESS'
                AND created_at >= ?
                AND created_at < ?
              GROUP BY payer_hash
              HAVING COUNT(*) > 1
            ) t
            """,
            (merchant_id, last_7_start.isoformat(), today_end.isoformat()),
        ).fetchone()
        repeat_customers_week = int(rep_row["repeat_payers"] or 0) if rep_row else 0

        # Top hour historically: hour with most transactions (SUCCESS only)
        hour_rows = list(
            conn.execute(
                """
                SELECT created_at
                FROM transactions
                WHERE merchant_id = ?
                  AND status = 'SUCCESS'
                """,
                (merchant_id,),
            )
        )
        hour_counts: dict[int, int] = {}
        for r in hour_rows:
            dt = _parse_dt(str(r["created_at"])).astimezone(IST)
            hour_counts[dt.hour] = hour_counts.get(dt.hour, 0) + 1
        if hour_counts:
            top_hour = int(max(hour_counts.items(), key=lambda kv: kv[1])[0])

    finally:
        conn.close()

    credit_score = int(_load_credit_score(merchant_id=merchant_id))

    active_alerts: list[str] = []
    if wow_delta_pct <= -20.0:
        active_alerts.append("REVENUE_DROP_WOW")
    if velocity_drop:
        active_alerts.append("VELOCITY_DROP")
    if ticket_drop:
        active_alerts.append("TICKET_DROP")
    if today_revenue == 0.0:
        active_alerts.append("NO_REVENUE_TODAY")

    if today_revenue > 0.0:
        recharge_share = today_recharge_revenue / today_revenue if today_revenue else 0.0
        if recharge_share >= 0.6:
            active_alerts.append("HIGH_RECHARGE_SHARE")

    return {
        "merchant_id": merchant_id,
        "merchant_type": "kirana",
        "location": "Durg, Chhattisgarh",
        "today_revenue": float(today_revenue),
        "today_recharge_revenue": float(today_recharge_revenue),
        "today_goods_revenue": float(today_goods_revenue),
        "wow_delta_pct": float(wow_delta_pct),
        "txn_count_today": int(txn_count_today),
        "txn_7day_avg": float(txn_7day_avg),
        "velocity_drop": bool(velocity_drop),
        "avg_ticket_today": float(avg_ticket_today),
        "avg_ticket_4week": float(avg_ticket_4week),
        "ticket_drop": bool(ticket_drop),
        "repeat_customers_week": int(repeat_customers_week),
        "top_hour": int(top_hour),
        "credit_score": int(credit_score),
        "active_alerts": active_alerts,
    }

