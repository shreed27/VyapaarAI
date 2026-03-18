#!/usr/bin/env python3
"""
Generate local SQLite datasets for VyapaarAI.

Creates:
- transactions.db: 500 transactions over the last 90 days
- merchants.db: 5000 merchant profiles for ML training

The generator is deterministic if you pass --seed.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import sqlite3
import string
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence


RECHARGE_AMOUNTS = (149, 199, 239, 299, 399, 479, 599)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Keep generation clean: avoid creating -wal/-shm files.
    conn.execute("PRAGMA journal_mode=DELETE;")
    conn.execute("PRAGMA synchronous=FULL;")
    return conn


def _rand_str(rng: random.Random, n: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(rng.choice(alphabet) for _ in range(n))


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _weighted_choice(rng: random.Random, values: Sequence[int], weights: Sequence[float]) -> int:
    # random.choices is fine, but keep it explicit and easy to unit test.
    total = float(sum(weights))
    r = rng.random() * total
    upto = 0.0
    for v, w in zip(values, weights):
        upto += float(w)
        if upto >= r:
            return v
    return values[-1]


def _sample_recent_datetime_ist(rng: random.Random, now_ist: datetime, days: int) -> datetime:
    """
    Create a realistic timestamp distribution for a kirana:
    - more volume on weekends
    - peaks late morning and evening
    """
    # Day selection with weekend boost.
    # Pick an offset day in [0, days-1] where 0 means today.
    # Weight recent days slightly higher to mimic more recent usage.
    offsets = list(range(days))
    day_weights = []
    for d in offsets:
        day = (now_ist.date() - timedelta(days=d))
        # Weekend boost
        weekend = 1.2 if day.weekday() >= 5 else 1.0
        # Recency boost (today slightly more likely than 3 months ago)
        recency = 1.0 + (days - d) / (days * 6.0)
        day_weights.append(weekend * recency)
    day_offset = _weighted_choice(rng, offsets, day_weights)
    chosen_date = now_ist.date() - timedelta(days=day_offset)

    # Hour distribution: peaks ~11-13 and ~18-21, low at night.
    hours = list(range(24))
    hour_weights = []
    for h in hours:
        if 10 <= h <= 13:
            w = 3.0
        elif 17 <= h <= 21:
            w = 3.5
        elif 7 <= h <= 9 or 14 <= h <= 16:
            w = 1.8
        elif 22 <= h <= 23:
            w = 1.0
        else:  # 0-6
            w = 0.25
        hour_weights.append(w)
    hour = _weighted_choice(rng, hours, hour_weights)

    # Minute distribution: small bumps around :00, :15, :30, :45
    minute = int(rng.random() * 60)
    if rng.random() < 0.25:
        minute = rng.choice([0, 15, 30, 45])
    second = int(rng.random() * 60)

    tz_ist = timezone(timedelta(hours=5, minutes=30))
    return datetime(chosen_date.year, chosen_date.month, chosen_date.day, hour, minute, second, tzinfo=tz_ist)


def _amount_goods(rng: random.Random) -> int:
    # Skewed ticket sizes with a long tail: common 40-300, occasional 500-1500.
    p = rng.random()
    if p < 0.70:
        return int(rng.triangular(40, 320, 110))
    if p < 0.92:
        return int(rng.triangular(120, 700, 260))
    return int(rng.triangular(400, 1800, 900))


def _create_merchants_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS merchants;
        CREATE TABLE merchants (
          merchant_id TEXT PRIMARY KEY,
          shop_name TEXT NOT NULL,
          category TEXT NOT NULL,
          city TEXT NOT NULL,
          state TEXT NOT NULL,
          pincode TEXT NOT NULL,
          created_at TEXT NOT NULL,
          typical_ticket REAL NOT NULL,
          avg_daily_tx REAL NOT NULL,
          recharge_affinity REAL NOT NULL
        );
        """
    )


def _create_transactions_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS transactions;
        CREATE TABLE transactions (
          tx_id TEXT PRIMARY KEY,
          merchant_id TEXT NOT NULL,
          payer_hash TEXT NOT NULL,
          amount INTEGER NOT NULL,
          status TEXT NOT NULL, -- SUCCESS | FAILED
          is_recharge INTEGER NOT NULL, -- 0/1
          created_at TEXT NOT NULL
        );
        CREATE INDEX idx_transactions_merchant_created ON transactions(merchant_id, created_at);
        CREATE INDEX idx_transactions_payer ON transactions(payer_hash);
        """
    )


@dataclass(frozen=True)
class MerchantProfile:
    merchant_id: str
    shop_name: str
    category: str
    city: str
    state: str
    pincode: str
    created_at: str
    typical_ticket: float
    avg_daily_tx: float
    recharge_affinity: float


def generate_merchants(rng: random.Random, n: int, now_ist: datetime) -> list[MerchantProfile]:
    categories = ["kirana", "pharmacy", "stationery", "dairy", "electronics", "restaurant", "salon"]
    cities = [
        ("Mumbai", "Maharashtra"),
        ("Pune", "Maharashtra"),
        ("Delhi", "Delhi"),
        ("Bengaluru", "Karnataka"),
        ("Hyderabad", "Telangana"),
        ("Chennai", "Tamil Nadu"),
        ("Kolkata", "West Bengal"),
        ("Jaipur", "Rajasthan"),
        ("Lucknow", "Uttar Pradesh"),
        ("Indore", "Madhya Pradesh"),
        ("Surat", "Gujarat"),
        ("Ahmedabad", "Gujarat"),
        ("Kochi", "Kerala"),
        ("Bhopal", "Madhya Pradesh"),
        ("Patna", "Bihar"),
    ]

    merchants: list[MerchantProfile] = []
    for i in range(n):
        # Use stable ids for the first 200 merchants so other scripts can
        # reliably reference merchant_001, merchant_002, ... (useful for demos).
        if i < 200:
            merchant_id = f"merchant_{i+1:03d}"
        else:
            merchant_id = _sha256_hex(f"m|{i}|{_rand_str(rng, 12)}")[:32]
        city, state = rng.choice(cities)
        category = rng.choices(categories, weights=[4.5, 1.2, 1.0, 1.3, 0.8, 1.0, 0.7], k=1)[0]

        # "created_at" within last ~2 years.
        created_dt = now_ist - timedelta(days=int(rng.triangular(1, 730, 220)))
        created_at = created_dt.isoformat()

        typical_ticket = float(max(25.0, rng.triangular(35, 650, 140)))
        avg_daily_tx = float(max(0.5, rng.triangular(2, 120, 18)))
        recharge_affinity = float(min(1.0, max(0.0, rng.betavariate(2.2, 5.0))))
        pincode = f"{rng.randint(100000, 999999)}"
        shop_name = f"{city} {_rand_str(rng, 6)} {category.title()}"

        merchants.append(
            MerchantProfile(
                merchant_id=merchant_id,
                shop_name=shop_name,
                category=category,
                city=city,
                state=state,
                pincode=pincode,
                created_at=created_at,
                typical_ticket=typical_ticket,
                avg_daily_tx=avg_daily_tx,
                recharge_affinity=recharge_affinity,
            )
        )
    return merchants


def write_merchants_db(db_path: str, merchants: Iterable[MerchantProfile]) -> None:
    conn = _connect(db_path)
    try:
        _create_merchants_schema(conn)
        conn.executemany(
            """
            INSERT INTO merchants(
              merchant_id, shop_name, category, city, state, pincode, created_at,
              typical_ticket, avg_daily_tx, recharge_affinity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    m.merchant_id,
                    m.shop_name,
                    m.category,
                    m.city,
                    m.state,
                    m.pincode,
                    m.created_at,
                    m.typical_ticket,
                    m.avg_daily_tx,
                    m.recharge_affinity,
                )
                for m in merchants
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _generate_payer_hashes(rng: random.Random, n: int) -> list[str]:
    # Simulate consistent payer identities without storing PII.
    # Use "phone-like" seeds to mimic UPI-linked wallets.
    hashes: list[str] = []
    seen = set()
    while len(hashes) < n:
        phone = f"+91{rng.randint(6000000000, 9999999999)}"
        h = _sha256_hex(f"payer|{phone}")[:24]
        if h in seen:
            continue
        seen.add(h)
        hashes.append(h)
    return hashes


def write_transactions_db(
    db_path: str,
    *,
    rng: random.Random,
    now_ist: datetime,
    merchant_ids: Sequence[str],
    payer_hashes: Sequence[str],
    n_transactions: int,
    days: int,
    failed_rate: float,
    recharge_amounts: Sequence[int],
) -> None:
    conn = _connect(db_path)
    try:
        _create_transactions_schema(conn)

        # Ensure demo merchants like merchant_001 get enough data for training.
        merchant_weights = [30.0 if mid == "merchant_001" else 1.0 for mid in merchant_ids]

        rows = []
        for i in range(n_transactions):
            merchant_id = rng.choices(list(merchant_ids), weights=merchant_weights, k=1)[0]
            payer_hash = rng.choice(payer_hashes)

            # Recharge detection by known plan amounts, with some probability.
            # Merchants with higher "recharge affinity" will naturally have more
            # recharge-like amounts; approximate without joining merchants table.
            is_recharge = 1 if rng.random() < 0.22 else 0
            if is_recharge:
                amount = int(rng.choice(recharge_amounts))
            else:
                amount = int(_amount_goods(rng))

            status = "FAILED" if rng.random() < failed_rate else "SUCCESS"
            created_at = _sample_recent_datetime_ist(rng, now_ist, days=days).isoformat()
            tx_id = _sha256_hex(f"tx|{i}|{merchant_id}|{payer_hash}|{created_at}|{_rand_str(rng, 8)}")[:32]

            rows.append((tx_id, merchant_id, payer_hash, amount, status, is_recharge, created_at))

        conn.executemany(
            """
            INSERT INTO transactions(tx_id, merchant_id, payer_hash, amount, status, is_recharge, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate VyapaarAI local SQLite datasets")
    default_out_dir = os.path.join("backend", "app", "data")
    parser.add_argument("--out-dir", default=default_out_dir, help=f"Output directory (default: {default_out_dir})")
    parser.add_argument("--seed", type=int, default=27, help="Random seed for reproducibility")
    parser.add_argument("--transactions", type=int, default=500, help="Number of transactions to generate")
    parser.add_argument("--days", type=int, default=90, help="How many days back to spread transactions")
    parser.add_argument("--failed-rate", type=float, default=0.15, help="Fraction of failed transactions")
    parser.add_argument("--unique-payers", type=int, default=80, help="Number of unique payer hashes")
    parser.add_argument("--merchant-profiles", type=int, default=5000, help="Number of merchant profiles for ML training")
    return parser.parse_args(list(argv))


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    rng = random.Random(args.seed)

    out_dir = args.out_dir
    _ensure_dir(out_dir)

    tz_ist = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(tz=tz_ist)

    merchants_db_path = os.path.join(out_dir, "merchants.db")
    transactions_db_path = os.path.join(out_dir, "transactions.db")

    merchants = generate_merchants(rng, args.merchant_profiles, now_ist)
    write_merchants_db(merchants_db_path, merchants)

    # Use a subset of merchants for the transaction history (still present in merchants.db).
    merchant_ids = [m.merchant_id for m in merchants[:200]]
    payer_hashes = _generate_payer_hashes(rng, args.unique_payers)
    write_transactions_db(
        transactions_db_path,
        rng=rng,
        now_ist=now_ist,
        merchant_ids=merchant_ids,
        payer_hashes=payer_hashes,
        n_transactions=args.transactions,
        days=args.days,
        failed_rate=args.failed_rate,
        recharge_amounts=RECHARGE_AMOUNTS,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
