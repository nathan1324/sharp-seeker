"""Analyze Elite signal performance by signal type.

Retroactively checks graded signals against the current best combos
and best hours config to determine which would qualify as Elite (both
combo + hour match), then shows performance by signal type.

Usage:
    docker compose exec sharp-seeker python /app/scripts/analyze_elite_by_type.py
    docker compose exec sharp-seeker python /app/scripts/analyze_elite_by_type.py 2026-03-20
"""

import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

DB = os.getenv("DB_PATH", "/app/data/sharp_seeker.db")
MST = ZoneInfo("America/Phoenix")

# Parse configs from env
BEST_COMBOS = set(json.loads(os.getenv("SIGNAL_BEST_COMBOS", "[]")))
BEST_HOURS = {
    k: set(v)
    for k, v in json.loads(os.getenv("SIGNAL_BEST_HOURS", "{}")).items()
}

SIGNAL_LABELS = {
    "steam_move": "Steam",
    "rapid_change": "Rapid",
    "pinnacle_divergence": "PinDiv",
    "reverse_line": "RevLine",
    "exchange_shift": "ExchShift",
}


def compute_units(price, result, multiplier=1):
    if result == "push" or price is None:
        return 0.0
    if price < 0:
        risk = abs(price) / 100.0
    else:
        risk = 100.0 / price if price > 0 else 1.0
    if result == "won":
        return 1.0 * multiplier
    elif result == "lost":
        return -risk * multiplier
    return 0.0


def get_price(row):
    details_raw = row.get("details_json")
    if not details_raw:
        return None
    try:
        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        vb = details.get("value_books", [])
        return vb[0].get("price") if vb else None
    except (json.JSONDecodeError, TypeError):
        return None


def is_best_combo(signal_type, sport_key, market_key):
    key = "{st}:{sp}:{mk}".format(st=signal_type, sp=sport_key, mk=market_key)
    return key in BEST_COMBOS


def is_best_hour(signal_type, signal_at):
    hours = BEST_HOURS.get(signal_type)
    if not hours or not signal_at:
        return False
    try:
        dt = datetime.fromisoformat(signal_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        mst_hour = dt.astimezone(MST).hour
        return mst_hour in hours
    except (ValueError, TypeError):
        return False


def classify(row):
    """Return (is_combo, is_hour, qualifier_count)."""
    combo = is_best_combo(row["signal_type"], row["sport_key"], row["market_key"])
    hour = is_best_hour(row["signal_type"], row["signal_at"])
    q = int(combo) + int(hour)
    return combo, hour, q


def fmt(w, l, p, u):
    n = w + l + p
    decided = w + l
    if decided == 0:
        return "(n={n:4d})  --".format(n=n)
    rate = w / decided
    sign = "+" if u >= 0 else ""
    return "(n={n:4d})  {w}W-{l}L-{p}P  ({rate:.0%})  {sign}{u:.1f}u".format(
        n=n, w=w, l=l, p=p, rate=rate, sign=sign, u=u,
    )


def tally(rows):
    w = sum(1 for r in rows if r["result"] == "won")
    l = sum(1 for r in rows if r["result"] == "lost")
    p = sum(1 for r in rows if r["result"] == "push")
    u = sum(compute_units(get_price(r), r["result"]) for r in rows)
    return w, l, p, u


def section(title):
    print()
    print("=" * 70)
    print("  {t}".format(t=title))
    print("=" * 70)


def main():
    for attempt in range(10):
        try:
            conn = sqlite3.connect(DB, timeout=10)
            conn.row_factory = sqlite3.Row
            break
        except sqlite3.OperationalError:
            print("  DB locked, retrying ({a}/10)...".format(a=attempt + 1))
            time.sleep(3)
    else:
        raise SystemExit("ERROR: Could not acquire DB lock.")

    since = sys.argv[1] if len(sys.argv) > 1 else None
    if since:
        rows = [dict(r) for r in conn.execute("""
            SELECT event_id, sport_key, signal_type, market_key, outcome_name,
                   signal_strength, signal_at, result, details_json
            FROM signal_results
            WHERE result IS NOT NULL AND signal_at >= ?
            ORDER BY signal_at
        """, (since,)).fetchall()]
    else:
        rows = [dict(r) for r in conn.execute("""
            SELECT event_id, sport_key, signal_type, market_key, outcome_name,
                   signal_strength, signal_at, result, details_json
            FROM signal_results
            WHERE result IS NOT NULL
            ORDER BY signal_at
        """).fetchall()]
    conn.close()

    if not rows:
        print("No graded signals found.")
        return

    since_label = " (since {s})".format(s=since) if since else ""
    print("Analyzing {n} graded signals{l}".format(n=len(rows), l=since_label))

    print("Config: {c} best combos, {h} best hour slots".format(
        c=len(BEST_COMBOS),
        h=sum(len(v) for v in BEST_HOURS.values()),
    ))
    print("Combos: {c}".format(c=sorted(BEST_COMBOS)))
    print("Hours: {h}".format(h=BEST_HOURS))

    # Classify each row
    for row in rows:
        row["_combo"], row["_hour"], row["_q"] = classify(row)

    total = len(rows)
    elite = [r for r in rows if r["_q"] >= 2]
    one_q = [r for r in rows if r["_q"] == 1]
    zero_q = [r for r in rows if r["_q"] == 0]

    section("1. OVERALL BY QUALIFIER COUNT")
    w, l, p, u = tally(elite)
    print("  Elite (2q):     {f}".format(f=fmt(w, l, p, u)))
    w, l, p, u = tally(one_q)
    print("  1 qualifier:    {f}".format(f=fmt(w, l, p, u)))
    w, l, p, u = tally(zero_q)
    print("  0 qualifiers:   {f}".format(f=fmt(w, l, p, u)))

    # ── 2. Elite by signal type ──────────────────────────────
    section("2. ELITE (2q) BY SIGNAL TYPE")
    types = sorted(set(r["signal_type"] for r in rows))
    for st in types:
        st_rows = [r for r in elite if r["signal_type"] == st]
        if not st_rows:
            continue
        w, l, p, u = tally(st_rows)
        label = SIGNAL_LABELS.get(st, st)
        print("  {label:12s} {f}".format(label=label, f=fmt(w, l, p, u)))

    # ── 3. Elite by signal type x sport ──────────────────────
    section("3. ELITE (2q) BY SIGNAL TYPE x SPORT")
    for st in types:
        st_elite = [r for r in elite if r["signal_type"] == st]
        if not st_elite:
            continue
        label = SIGNAL_LABELS.get(st, st)
        print("\n  {l}:".format(l=label))
        sports = sorted(set(r["sport_key"] for r in st_elite))
        for sp in sports:
            sp_rows = [r for r in st_elite if r["sport_key"] == sp]
            w, l, p, u = tally(sp_rows)
            sp_short = sp.split("_")[-1].upper()
            print("    {sp:12s} {f}".format(sp=sp_short, f=fmt(w, l, p, u)))

    # ── 4. Elite by signal type x market ─────────────────────
    section("4. ELITE (2q) BY SIGNAL TYPE x MARKET")
    mkt_short = {"h2h": "ML", "spreads": "Spread", "totals": "Total"}
    for st in types:
        st_elite = [r for r in elite if r["signal_type"] == st]
        if not st_elite:
            continue
        label = SIGNAL_LABELS.get(st, st)
        print("\n  {l}:".format(l=label))
        markets = sorted(set(r["market_key"] for r in st_elite))
        for mk in markets:
            mk_rows = [r for r in st_elite if r["market_key"] == mk]
            w, l, p, u = tally(mk_rows)
            mk_name = mkt_short.get(mk, mk)
            print("    {m:12s} {f}".format(m=mk_name, f=fmt(w, l, p, u)))

    # ── 5. 1-qualifier by signal type (for comparison) ───────
    section("5. ONE QUALIFIER (1q) BY SIGNAL TYPE")
    for st in types:
        st_rows = [r for r in one_q if r["signal_type"] == st]
        if not st_rows:
            continue
        w, l, p, u = tally(st_rows)
        label = SIGNAL_LABELS.get(st, st)
        print("  {label:12s} {f}".format(label=label, f=fmt(w, l, p, u)))

    # ── 6. Stable period only (Mar 3-14) ─────────────────────
    section("6. ELITE (2q) BY TYPE — STABLE PERIOD ONLY (Mar 3-14)")
    stable = [r for r in elite if "2026-03-03" <= r["signal_at"] < "2026-03-15"]
    for st in types:
        st_rows = [r for r in stable if r["signal_type"] == st]
        if not st_rows:
            continue
        w, l, p, u = tally(st_rows)
        label = SIGNAL_LABELS.get(st, st)
        print("  {label:12s} {f}".format(label=label, f=fmt(w, l, p, u)))

    if not stable:
        print("  (no Elite signals in stable period)")

    # ── 7. Each best combo performance ───────────────────────
    section("7. PERFORMANCE BY BEST COMBO (all signals matching)")
    for combo_key in sorted(BEST_COMBOS):
        parts = combo_key.split(":")
        if len(parts) != 3:
            continue
        st, sp, mk = parts
        combo_rows = [
            r for r in rows
            if r["signal_type"] == st and r["sport_key"] == sp and r["market_key"] == mk
        ]
        if not combo_rows:
            continue
        w, l, p, u = tally(combo_rows)
        print("  {k:55s} {f}".format(k=combo_key, f=fmt(w, l, p, u)))

    # ── 8. Each best hour performance ────────────────────────
    section("8. PERFORMANCE BY BEST HOUR (all signals matching)")
    for st, hours in sorted(BEST_HOURS.items()):
        label = SIGNAL_LABELS.get(st, st)
        for h in sorted(hours):
            h_rows = [
                r for r in rows
                if r["signal_type"] == st and is_best_hour(st, r["signal_at"])
            ]
            # Filter to just this hour
            h_only = []
            for r in rows:
                if r["signal_type"] != st:
                    continue
                try:
                    dt = datetime.fromisoformat(r["signal_at"])
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt.astimezone(MST).hour == h:
                        h_only.append(r)
                except (ValueError, TypeError):
                    continue
            if not h_only:
                continue
            w, l, p, u = tally(h_only)
            print("  {l:8s} {h:2d}:00 MST  {f}".format(l=label, h=h, f=fmt(w, l, p, u)))

    print()
    print("Done.")


if __name__ == "__main__":
    main()
