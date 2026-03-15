"""Comprehensive signal performance analysis.

Breaks down ALL signal types across every useful dimension to find
what's working, what to ignore, and where to lean in.

Sections:
  1. Overall by signal type
  2. Signal type × sport
  3. Signal type × market
  4. Signal type × sport × market (combo matrix)
  5. Signal type × strength bucket
  6. Signal type × hour (MST)
  7. Top/bottom combos ranked by hit rate (min sample)
  8. Actionable summary
"""

import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))

STRENGTH_BUCKETS = [
    (0.0, 0.33, "<33%"),
    (0.33, 0.50, "33-49%"),
    (0.50, 0.67, "50-66%"),
    (0.67, 0.80, "67-79%"),
    (0.80, 1.01, "80%+"),
]

SIGNAL_LABELS = {
    "steam_move": "Steam",
    "rapid_change": "Rapid",
    "pinnacle_divergence": "PinDiv",
    "reverse_line": "RevLine",
    "exchange_shift": "ExchShift",
}

SPORT_SHORT = {
    "basketball_nba": "NBA",
    "basketball_ncaab": "NCAAB",
    "icehockey_nhl": "NHL",
}

MARKET_SHORT = {
    "h2h": "ML",
    "spreads": "Spread",
    "totals": "Total",
}

MIN_SAMPLE = 10  # minimum signals for a combo to appear in rankings


def connect():
    for attempt in range(10):
        try:
            conn = sqlite3.connect(DB, timeout=10)
            conn.row_factory = sqlite3.Row
            conn.execute("SELECT 1 FROM signal_results LIMIT 1")
            return conn
        except sqlite3.OperationalError:
            print(f"  DB locked, retrying ({attempt + 1}/10)...")
            time.sleep(3)
    raise SystemExit("ERROR: Could not acquire DB lock after 10 attempts.")


def fmt(wins, losses, pushes):
    n = wins + losses + pushes
    decided = wins + losses
    if decided == 0:
        return f"(n={n:4d})  {wins}W-{losses}L-{pushes}P  (--)"
    rate = wins / decided
    return f"(n={n:4d})  {wins}W-{losses}L-{pushes}P  ({rate:.0%})"


def rate(wins, losses):
    decided = wins + losses
    if decided == 0:
        return 0.0
    return wins / decided


def section(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def strength_label(s):
    for lo, hi, label in STRENGTH_BUCKETS:
        if lo <= s < hi:
            return label
    return "?"


def mst_label(utc_hour):
    mst_hour = (utc_hour - 7) % 24
    ampm = "AM" if mst_hour < 12 else "PM"
    display = mst_hour % 12 or 12
    return f"{display:2d} {ampm} MST"


def tally(rows, key_fn):
    """Group rows by key_fn and tally W/L/P."""
    buckets = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for row in rows:
        k = key_fn(row)
        if k is not None:
            buckets[k][row["result"]] += 1
    return buckets


def print_buckets(buckets, key_order=None, label_fn=None, indent=2):
    """Print a tally dict with optional ordering and label formatting."""
    if key_order is None:
        key_order = sorted(buckets.keys())
    pad = " " * indent
    for k in key_order:
        if k not in buckets:
            continue
        d = buckets[k]
        label = label_fn(k) if label_fn else str(k)
        print(f"{pad}{label:42s} {fmt(d['won'], d['lost'], d['push'])}")


def run():
    conn = connect()

    since = sys.argv[1] if len(sys.argv) > 1 else None
    if since:
        cur = conn.execute("""
            SELECT event_id, sport_key, signal_type, market_key, outcome_name,
                   signal_strength, signal_at, result, details_json
            FROM signal_results
            WHERE result IS NOT NULL AND signal_at >= ?
            ORDER BY signal_at
        """, (since,))
    else:
        cur = conn.execute("""
            SELECT event_id, sport_key, signal_type, market_key, outcome_name,
                   signal_strength, signal_at, result, details_json
            FROM signal_results
            WHERE result IS NOT NULL
            ORDER BY signal_at
        """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    if not rows:
        print("No graded signals found.")
        return

    # Parse datetimes
    for row in rows:
        row["dt"] = datetime.fromisoformat(row["signal_at"])
        row["dt_mst"] = row["dt"].astimezone(MST)

    total = len(rows)
    first = rows[0]["dt_mst"]
    last = rows[-1]["dt_mst"]
    print(f"Analyzing {total} graded signals (all types)")
    print(f"Range: {first.strftime('%m/%d %I:%M %p')} — {last.strftime('%m/%d %I:%M %p')} MST")

    # ── 1. Overall by signal type ───────────────────────────
    section("1. OVERALL BY SIGNAL TYPE")
    by_type = tally(rows, lambda r: r["signal_type"])
    sig_types = sorted(by_type.keys())
    print_buckets(by_type, sig_types, lambda k: SIGNAL_LABELS.get(k, k))

    # ── 2. Signal type × sport ──────────────────────────────
    section("2. SIGNAL TYPE x SPORT")
    by_type_sport = tally(rows, lambda r: (r["signal_type"], r["sport_key"]))
    keys = sorted(by_type_sport.keys())
    print_buckets(
        by_type_sport, keys,
        lambda k: f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {SPORT_SHORT.get(k[1], k[1])}"
    )

    # ── 3. Signal type × market ─────────────────────────────
    section("3. SIGNAL TYPE x MARKET")
    by_type_market = tally(rows, lambda r: (r["signal_type"], r["market_key"]))
    keys = sorted(by_type_market.keys())
    print_buckets(
        by_type_market, keys,
        lambda k: f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {MARKET_SHORT.get(k[1], k[1])}"
    )

    # ── 4. Signal type × sport × market ─────────────────────
    section("4. SIGNAL TYPE x SPORT x MARKET (full combo)")
    by_combo = tally(rows, lambda r: (r["signal_type"], r["sport_key"], r["market_key"]))
    keys = sorted(by_combo.keys())
    print_buckets(
        by_combo, keys,
        lambda k: f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {SPORT_SHORT.get(k[1], k[1]):6s} {MARKET_SHORT.get(k[2], k[2])}"
    )

    # ── 5. Signal type × strength bucket ────────────────────
    section("5. SIGNAL TYPE x STRENGTH")
    by_type_str = tally(
        rows,
        lambda r: (r["signal_type"], strength_label(r["signal_strength"]))
    )
    for st in sig_types:
        print(f"\n  {SIGNAL_LABELS.get(st, st)}:")
        for _, _, slabel in STRENGTH_BUCKETS:
            k = (st, slabel)
            if k in by_type_str:
                d = by_type_str[k]
                print(f"    {slabel:10s}  {fmt(d['won'], d['lost'], d['push'])}")

    # ── 6. Signal type × hour (MST) ────────────────────────
    section("6. SIGNAL TYPE x HOUR (MST)")
    by_type_hour = tally(
        rows,
        lambda r: (r["signal_type"], r["dt"].hour)
    )
    for st in sig_types:
        print(f"\n  {SIGNAL_LABELS.get(st, st)}:")
        for utc_h in range(24):
            k = (st, utc_h)
            if k in by_type_hour:
                d = by_type_hour[k]
                n = d["won"] + d["lost"] + d["push"]
                decided = d["won"] + d["lost"]
                pct = f"{d['won']/decided:.0%}" if decided else "--"
                print(f"    {mst_label(utc_h):10s}  (n={n:3d})  {d['won']}W-{d['lost']}L  ({pct})")

    # ── 7. Ranked combos (min N sample) ─────────────────────
    section(f"7. BEST & WORST COMBOS (n >= {MIN_SAMPLE})")

    # Build combo list: signal_type × sport × market
    combos = []
    for k, d in by_combo.items():
        decided = d["won"] + d["lost"]
        if decided >= MIN_SAMPLE:
            combos.append({
                "key": k,
                "label": f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {SPORT_SHORT.get(k[1], k[1]):6s} {MARKET_SHORT.get(k[2], k[2])}",
                "won": d["won"],
                "lost": d["lost"],
                "push": d["push"],
                "n": decided + d["push"],
                "rate": rate(d["won"], d["lost"]),
            })

    if combos:
        combos.sort(key=lambda c: c["rate"], reverse=True)
        print("\n  BEST:")
        for c in combos[:8]:
            print(f"    {c['label']:35s} {fmt(c['won'], c['lost'], c['push'])}  {'***' if c['rate'] >= 0.55 else ''}")

        print("\n  WORST:")
        for c in combos[-5:]:
            print(f"    {c['label']:35s} {fmt(c['won'], c['lost'], c['push'])}  {'!!!' if c['rate'] < 0.45 else ''}")
    else:
        print(f"  No combos with n >= {MIN_SAMPLE}")

    # Also rank signal_type × strength
    section(f"7b. BEST & WORST: SIGNAL TYPE x STRENGTH (n >= {MIN_SAMPLE})")
    str_combos = []
    for k, d in by_type_str.items():
        decided = d["won"] + d["lost"]
        if decided >= MIN_SAMPLE:
            str_combos.append({
                "label": f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {k[1]}",
                "won": d["won"],
                "lost": d["lost"],
                "push": d["push"],
                "n": decided + d["push"],
                "rate": rate(d["won"], d["lost"]),
            })

    if str_combos:
        str_combos.sort(key=lambda c: c["rate"], reverse=True)
        print("\n  BEST:")
        for c in str_combos[:6]:
            print(f"    {c['label']:30s} {fmt(c['won'], c['lost'], c['push'])}  {'***' if c['rate'] >= 0.55 else ''}")
        print("\n  WORST:")
        for c in str_combos[-4:]:
            print(f"    {c['label']:30s} {fmt(c['won'], c['lost'], c['push'])}  {'!!!' if c['rate'] < 0.45 else ''}")
    else:
        print(f"  No combos with n >= {MIN_SAMPLE}")

    # Also rank signal_type × hour
    section(f"7c. BEST & WORST: SIGNAL TYPE x HOUR (n >= {MIN_SAMPLE})")
    hour_combos = []
    for k, d in by_type_hour.items():
        decided = d["won"] + d["lost"]
        if decided >= MIN_SAMPLE:
            hour_combos.append({
                "label": f"{SIGNAL_LABELS.get(k[0], k[0]):10s} {mst_label(k[1])}",
                "won": d["won"],
                "lost": d["lost"],
                "push": d["push"],
                "n": decided + d["push"],
                "rate": rate(d["won"], d["lost"]),
            })

    if hour_combos:
        hour_combos.sort(key=lambda c: c["rate"], reverse=True)
        print("\n  BEST:")
        for c in hour_combos[:8]:
            print(f"    {c['label']:30s} {fmt(c['won'], c['lost'], c['push'])}  {'***' if c['rate'] >= 0.55 else ''}")
        print("\n  WORST:")
        for c in hour_combos[-5:]:
            print(f"    {c['label']:30s} {fmt(c['won'], c['lost'], c['push'])}  {'!!!' if c['rate'] < 0.45 else ''}")
    else:
        print(f"  No combos with n >= {MIN_SAMPLE}")

    # ── 8. Summary ──────────────────────────────────────────
    section("8. ACTIONABLE SUMMARY")
    print()
    print("  Signal types ranked by hit rate:")
    type_ranked = []
    for st in sig_types:
        d = by_type[st]
        decided = d["won"] + d["lost"]
        if decided > 0:
            type_ranked.append((st, rate(d["won"], d["lost"]), decided))
    type_ranked.sort(key=lambda x: x[1], reverse=True)
    for st, r, n in type_ranked:
        flag = " <-- strong" if r >= 0.55 else " <-- weak" if r < 0.47 else ""
        print(f"    {SIGNAL_LABELS.get(st, st):12s} {r:.0%}  (n={n}){flag}")

    print()
    print(f"  Total graded: {total}")
    all_won = sum(1 for r in rows if r["result"] == "won")
    all_lost = sum(1 for r in rows if r["result"] == "lost")
    print(f"  Overall: {all_won}W-{all_lost}L ({rate(all_won, all_lost):.0%})")
    print()
    print("Done.")


if __name__ == "__main__":
    run()
