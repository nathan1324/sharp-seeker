"""Analyze signal win rates by time of day, signal type, sport, and date range.

Usage:
    python analyze_by_hour.py                # default: last 7 days as "recent"
    python analyze_by_hour.py --since 2026-02-23   # custom cutoff date
"""

import argparse
import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository


def _sport_label(sport_key):
    parts = sport_key.split("_", 1)
    return parts[-1].upper() if len(parts) > 1 else sport_key.upper()


def _pct(won, total):
    return f"{won / total * 100:.1f}%" if total else "N/A"


def _record(b):
    return f"{b['won']}W-{b['lost']}L-{b['push']}P ({_pct(b['won'], b['won'] + b['lost'])})"


async def main():
    parser = argparse.ArgumentParser(description="Analyze signal win rates")
    parser.add_argument(
        "--since", type=str, default=None,
        help="Cutoff date (YYYY-MM-DD) for recent vs older. Default: 7 days ago.",
    )
    args = parser.parse_args()

    if args.since:
        cutoff = args.since
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

    s = Settings()
    db = await init_db(s.db_path)
    repo = Repository(db)

    rows = await repo.get_resolved_signals_since("2000-01-01T00:00:00+00:00")
    if not rows:
        print("No resolved signals found.")
        return

    # Parse all rows
    signals = []
    for row in rows:
        d = dict(row)
        signal_at = d.get("signal_at", "")
        hour_utc = int(signal_at[11:13]) if len(signal_at) >= 13 else -1
        signal_date = signal_at[:10] if len(signal_at) >= 10 else ""
        signals.append({
            "result": d["result"].lower(),
            "sport": _sport_label(d.get("sport_key", "")),
            "signal_type": d["signal_type"],
            "market": d["market_key"],
            "hour_utc": hour_utc,
            "signal_date": signal_date,
        })

    await db.close()

    # ── Helper to bucket results ──
    def bucket(sigs):
        b = {"won": 0, "lost": 0, "push": 0, "total": 0}
        for s in sigs:
            r = s["result"]
            if r in b:
                b[r] += 1
            b["total"] += 1
        return b

    # ── Overall ──
    print("=" * 60)
    print(f"TOTAL SIGNALS: {len(signals)}")
    b = bucket(signals)
    print(f"Overall: {_record(b)}")
    print()

    # ── By date ──
    dates = sorted(set(s["signal_date"] for s in signals))
    print("=" * 60)
    print("BY DATE")
    print("-" * 60)
    for date in dates:
        sigs = [s for s in signals if s["signal_date"] == date]
        b = bucket(sigs)
        print(f"  {date}: {_record(b)}  (n={b['total']})")
    print()

    # ── Older vs Recent (dynamic cutoff) ──
    older = [s for s in signals if s["signal_date"] < cutoff]
    recent = [s for s in signals if s["signal_date"] >= cutoff]
    print("=" * 60)
    print(f"OLDER vs RECENT (cutoff: {cutoff})")
    print("-" * 60)
    if older:
        b = bucket(older)
        print(f"  Older  (<{cutoff}): {_record(b)}  (n={b['total']})")
    if recent:
        b = bucket(recent)
        print(f"  Recent (>={cutoff}): {_record(b)}  (n={b['total']})")
    print()

    # ── By hour (UTC) — all data ──
    hours = sorted(set(s["hour_utc"] for s in signals if s["hour_utc"] >= 0))
    print("=" * 60)
    print("BY HOUR (UTC) — All dates")
    print("  UTC  |  MT   | Record")
    print("-" * 60)
    for h in hours:
        mt = (h - 7) % 24
        sigs = [s for s in signals if s["hour_utc"] == h]
        b = bucket(sigs)
        print(f"  {h:02d}:00 | {mt:02d}:00 | {_record(b)}  (n={b['total']})")
    print()

    # ── By hour (UTC) — recent only ──
    if recent:
        hours_recent = sorted(set(s["hour_utc"] for s in recent if s["hour_utc"] >= 0))
        print("=" * 60)
        print(f"BY HOUR (UTC) — Recent (>={cutoff}) only")
        print("  UTC  |  MT   | Record")
        print("-" * 60)
        for h in hours_recent:
            mt = (h - 7) % 24
            sigs = [s for s in recent if s["hour_utc"] == h]
            b = bucket(sigs)
            print(f"  {h:02d}:00 | {mt:02d}:00 | {_record(b)}  (n={b['total']})")
        print()

    # ── By signal type ──
    types = sorted(set(s["signal_type"] for s in signals))
    print("=" * 60)
    print("BY SIGNAL TYPE — All dates")
    print("-" * 60)
    for t in types:
        sigs = [s for s in signals if s["signal_type"] == t]
        b = bucket(sigs)
        print(f"  {t}: {_record(b)}  (n={b['total']})")
    print()

    if recent:
        print("=" * 60)
        print(f"BY SIGNAL TYPE — Recent (>={cutoff}) only")
        print("-" * 60)
        for t in types:
            sigs = [s for s in recent if s["signal_type"] == t]
            if not sigs:
                continue
            b = bucket(sigs)
            print(f"  {t}: {_record(b)}  (n={b['total']})")
        print()

    # ── By sport ──
    sports = sorted(set(s["sport"] for s in signals))
    print("=" * 60)
    print("BY SPORT — All dates")
    print("-" * 60)
    for sp in sports:
        sigs = [s for s in signals if s["sport"] == sp]
        b = bucket(sigs)
        print(f"  {sp}: {_record(b)}  (n={b['total']})")
    print()

    # ── By market type ──
    markets = sorted(set(s["market"] for s in signals))
    print("=" * 60)
    print("BY MARKET — All dates")
    print("-" * 60)
    for m in markets:
        sigs = [s for s in signals if s["market"] == m]
        b = bucket(sigs)
        print(f"  {m}: {_record(b)}  (n={b['total']})")
    print()

    # ── Signal type × hour (recent) ──
    if recent:
        print("=" * 60)
        print(f"SIGNAL TYPE × HOUR — Recent (>={cutoff})")
        print("-" * 60)
        for t in types:
            t_sigs = [s for s in recent if s["signal_type"] == t]
            if not t_sigs:
                continue
            print(f"\n  {t}:")
            t_hours = sorted(set(s["hour_utc"] for s in t_sigs if s["hour_utc"] >= 0))
            for h in t_hours:
                mt = (h - 7) % 24
                sigs = [s for s in t_sigs if s["hour_utc"] == h]
                b = bucket(sigs)
                print(f"    {h:02d}:00 UTC ({mt:02d}:00 MT): {_record(b)}  (n={b['total']})")
        print()


if __name__ == "__main__":
    asyncio.run(main())
