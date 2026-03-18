#!/usr/bin/env python3
"""
Train an IsolationForest anomaly model from transactions.db (run once pre-hackathon).

Data source:
  backend/app/data/transactions.db  (or override via --transactions-db)

Training target:
  merchant_001 (or override via --merchant-id)

Features per transaction:
  - amount
  - hour_of_day (0-23)
  - day_of_week (0-6, Monday=0)
  - is_recharge (0/1)
  - days_since_account_start (based on the earliest tx date for that merchant)

Model:
  IsolationForest(n_estimators=100, contamination=0.05, random_state=42)

Output:
  backend/models/anomaly_model.pkl (or override via --out)
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


def parse_args(argv: list[str]) -> argparse.Namespace:
    default_db = os.path.join("backend", "app", "data", "transactions.db")
    default_out = os.path.join("backend", "models", "anomaly_model.pkl")
    p = argparse.ArgumentParser(description="Train IsolationForest anomaly model")
    p.add_argument("--transactions-db", default=default_db, help=f"Path to transactions.db (default: {default_db})")
    p.add_argument("--merchant-id", default="merchant_001", help="Merchant id to train for")
    p.add_argument("--out", default=default_out, help=f"Output model path (default: {default_out})")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    try:
        import joblib  # type: ignore
        import numpy as np  # type: ignore
        from sklearn.ensemble import IsolationForest  # type: ignore
    except Exception as e:
        print(
            "Missing dependencies. Install with:\n"
            "  python3 -m pip install -U scikit-learn joblib numpy\n"
            f"Original error: {e}",
            file=sys.stderr,
        )
        return 2

    if not os.path.exists(args.transactions_db):
        print(f"transactions.db not found: {args.transactions_db}", file=sys.stderr)
        return 2

    conn = _connect(args.transactions_db)
    try:
        merchant_exists = conn.execute(
            "SELECT 1 FROM transactions WHERE merchant_id = ? LIMIT 1", (args.merchant_id,)
        ).fetchone()
        if not merchant_exists:
            print(
                f"No transactions found for merchant_id={args.merchant_id}. "
                "Either generate data that includes this merchant id or pass --merchant-id.",
                file=sys.stderr,
            )
            return 2

        min_dt_row = conn.execute(
            "SELECT MIN(created_at) AS min_created_at FROM transactions WHERE merchant_id = ?",
            (args.merchant_id,),
        ).fetchone()
        if not min_dt_row or not min_dt_row["min_created_at"]:
            print(f"Could not determine account start for merchant_id={args.merchant_id}", file=sys.stderr)
            return 2
        start_dt = _parse_dt(str(min_dt_row["min_created_at"]))

        rows = list(
            conn.execute(
                """
                SELECT amount, is_recharge, created_at
                FROM transactions
                WHERE merchant_id = ?
                  AND status = 'SUCCESS'
                ORDER BY created_at ASC
                """,
                (args.merchant_id,),
            )
        )
    finally:
        conn.close()

    if len(rows) < 25:
        print(
            f"Not enough SUCCESS transactions for merchant_id={args.merchant_id} "
            f"to train a stable model (found {len(rows)}).",
            file=sys.stderr,
        )
        return 2

    X = []
    for r in rows:
        dt = _parse_dt(str(r["created_at"]))
        amount = float(r["amount"])
        hour_of_day = float(dt.hour)
        day_of_week = float(dt.weekday())
        is_recharge = float(int(r["is_recharge"]))
        days_since = float((dt.date() - start_dt.date()).days)
        X.append([amount, hour_of_day, day_of_week, is_recharge, days_since])

    X = np.asarray(X, dtype=float)

    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    joblib.dump(
        {
            "model": model,
            "merchant_id": args.merchant_id,
            "feature_order": [
                "amount",
                "hour_of_day",
                "day_of_week",
                "is_recharge",
                "days_since_account_start",
            ],
            "trained_at": datetime.utcnow().isoformat() + "Z",
        },
        args.out,
    )

    print(f"Saved model to {args.out} (n={len(rows)} transactions)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

