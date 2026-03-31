"""Cross-book hold analysis by sport.

Breaks down performance by hold bucket for each sport and signal type
to identify sport-specific hold windows worth filtering or promoting.
"""

import json
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"

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
    "baseball_mlb": "MLB",
}

MARKET_SHORT = {"h2h": "ML", "spreads": "Spread", "totals": "Total"}

# Fine-grained hold buckets for detailed analysis
HOLD_BUCKETS = [
    (-999, -5.0, "<-5%"),
    (-5.0, -3.0, "-5 to -3%"),
    (-3.0, -2.0, "-3 to -2%"),
    (-2.0, -1.0, "-2 to -1%"),
    (-1.0, 0.0, "-1 to 0%"),
    (0.0, 1.0, "0 to 1%"),
    (1.0, 2.0, "1 to 2%"),
    (2.0, 3.0, "2 to 3%"),
    (3.0, 4.0, "3 to 4%"),
    (4.0, 5.0, "4 to 5%"),
    (5.0, 7.0, "5 to 7%"),
    (7.0, 999, "7%+"),
]

# Coarse buckets for summary
COARSE_BUCKETS = [
    (-999, -2.0, "Arb (<-2%)"),
    (-2.0, 0.0, "Neg (-2 to 0%)"),
    (0.0, 2.0, "Tight (0-2%)"),
    (2.0, 4.0, "Edge (2-4%)"),
    (4.0, 999, "Wide (4%+)"),
]


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


def fmt(wins, losses, pushes, units=None):
    n = wins + losses + pushes
    decided = wins + losses
    if decided == 0:
        base = f"(n={n:4d})  {wins}W-{losses}L-{pushes}P  (--)"
    else:
        r = wins / decided
        base = f"(n={n:4d})  {wins}W-{losses}L-{pushes}P  ({r:.0%})"
    if units is not None:
        sign = "+" if units >= 0 else ""
        base += f"  [{sign}{units:.1f}u]"
    return base


def bucket_label(val, buckets):
    if val is None:
        return None
    for lo, hi, label in buckets:
        if lo <= val < hi:
            return label
    return "?"


def section(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def tally(rows, key_fn):
    buckets = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
    for row in rows:
        k = key_fn(row)
        if k is not None:
            buckets[k][row["result"]] += 1
            buckets[k]["units"] += compute_units(
                row.get("best_price"), row["result"], row.get("multiplier", 1),
            )
    return buckets


def print_buckets(buckets, key_order, label_fn=None, indent=6):
    pad = " " * indent
    for k in key_order:
        if k not in buckets:
            continue
        d = buckets[k]
        label = label_fn(k) if label_fn else str(k)
        print(f"{pad}{label:20s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")


def run():
    conn = connect()
    cur = conn.execute("""
        SELECT event_id, sport_key, signal_type, market_key, outcome_name,
               signal_strength, signal_at, result, details_json
        FROM signal_results
        WHERE result IS NOT NULL
        ORDER BY signal_at
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    # Enrich
    for row in rows:
        row["best_price"] = None
        row["multiplier"] = 1
        row["cross_hold"] = None
        row["us_hold"] = None
        details_raw = row.get("details_json")
        if details_raw:
            try:
                details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                row["cross_hold"] = details.get("cross_book_hold")
                row["us_hold"] = details.get("us_hold")
                vb = details.get("value_books", [])
                if vb:
                    row["best_price"] = vb[0].get("price")
                if details.get("qualifier_count", 0) >= 2:
                    row["multiplier"] = 2
            except (json.JSONDecodeError, TypeError):
                pass

    # Only rows with hold data
    hold_rows = [r for r in rows if r["cross_hold"] is not None]
    no_hold = [r for r in rows if r["cross_hold"] is None]

    print(f"Cross-Book Hold Analysis")
    print(f"Total signals: {len(rows)}")
    print(f"With hold data: {len(hold_rows)}")
    print(f"Without hold data: {len(no_hold)}")
    if hold_rows:
        first = hold_rows[0]["signal_at"][:10]
        last = hold_rows[-1]["signal_at"][:10]
        print(f"Hold data range: {first} to {last}")

    # ── 1. Overall by coarse hold ──────────────────────────
    section("1. OVERALL BY HOLD BUCKET")
    coarse_order = [b[2] for b in COARSE_BUCKETS]
    by_coarse = tally(hold_rows, lambda r: bucket_label(r["cross_hold"], COARSE_BUCKETS))
    print_buckets(by_coarse, coarse_order)

    # Fine-grained
    print("\n    Fine-grained:")
    fine_order = [b[2] for b in HOLD_BUCKETS]
    by_fine = tally(hold_rows, lambda r: bucket_label(r["cross_hold"], HOLD_BUCKETS))
    print_buckets(by_fine, fine_order)

    # ── 2. By sport (coarse) ──────────────────────────────
    section("2. BY SPORT")
    all_sports = sorted(set(r["sport_key"] for r in hold_rows))
    for sport in all_sports:
        sport_rows = [r for r in hold_rows if r["sport_key"] == sport]
        print(f"\n    {SPORT_SHORT.get(sport, sport)} ({len(sport_rows)} signals):")
        by_hold = tally(sport_rows, lambda r: bucket_label(r["cross_hold"], COARSE_BUCKETS))
        print_buckets(by_hold, coarse_order)

        # Fine-grained for this sport
        print(f"      Fine-grained:")
        by_fine_sport = tally(sport_rows, lambda r: bucket_label(r["cross_hold"], HOLD_BUCKETS))
        print_buckets(by_fine_sport, fine_order, indent=8)

    # ── 3. By sport x market ──────────────────────────────
    section("3. BY SPORT x MARKET")
    for sport in all_sports:
        for mkt in ["h2h", "spreads", "totals"]:
            sm_rows = [r for r in hold_rows if r["sport_key"] == sport and r["market_key"] == mkt]
            if not sm_rows:
                continue
            print(f"\n    {SPORT_SHORT.get(sport, sport)} {MARKET_SHORT[mkt]} ({len(sm_rows)} signals):")
            by_hold = tally(sm_rows, lambda r: bucket_label(r["cross_hold"], COARSE_BUCKETS))
            print_buckets(by_hold, coarse_order)

    # ── 4. By signal type x sport ─────────────────────────
    section("4. BY SIGNAL TYPE x SPORT")
    all_types = sorted(set(r["signal_type"] for r in hold_rows))
    for st in all_types:
        for sport in all_sports:
            ts_rows = [r for r in hold_rows if r["signal_type"] == st and r["sport_key"] == sport]
            if not ts_rows:
                continue
            print(f"\n    {SIGNAL_LABELS.get(st, st)} {SPORT_SHORT.get(sport, sport)} ({len(ts_rows)} signals):")
            by_hold = tally(ts_rows, lambda r: bucket_label(r["cross_hold"], COARSE_BUCKETS))
            print_buckets(by_hold, coarse_order)

    # ── 5. Actionable: hold ranges to suppress or promote ─
    section("5. ACTIONABLE WINDOWS")
    print()
    print("  Looking for hold ranges with n>=5 and clear directional signal...")
    print()

    for sport in all_sports:
        for mkt in ["h2h", "spreads", "totals"]:
            sm_rows = [r for r in hold_rows if r["sport_key"] == sport and r["market_key"] == mkt]
            if len(sm_rows) < 5:
                continue
            by_fine_sm = tally(sm_rows, lambda r: bucket_label(r["cross_hold"], HOLD_BUCKETS))
            bad_ranges = []
            good_ranges = []
            for label in fine_order:
                if label not in by_fine_sm:
                    continue
                d = by_fine_sm[label]
                decided = d["won"] + d["lost"]
                if decided < 3:
                    continue
                wr = d["won"] / decided
                if wr <= 0.40 and d["units"] < -2:
                    bad_ranges.append((label, d, wr))
                elif wr >= 0.58 and d["units"] > 2:
                    good_ranges.append((label, d, wr))

            if bad_ranges or good_ranges:
                print(f"  {SPORT_SHORT.get(sport, sport)} {MARKET_SHORT[mkt]}:")
                for label, d, wr in bad_ranges:
                    print(f"    SUPPRESS  {label:20s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")
                for label, d, wr in good_ranges:
                    print(f"    PROMOTE   {label:20s} {fmt(d['won'], d['lost'], d['push'], d['units'])}")
                print()

    # ── 6. Summary stats ──────────────────────────────────
    section("6. HOLD DISTRIBUTION SUMMARY")
    for sport in all_sports:
        sport_rows = [r for r in hold_rows if r["sport_key"] == sport]
        holds = [r["cross_hold"] for r in sport_rows]
        if holds:
            avg = sum(holds) / len(holds)
            median = sorted(holds)[len(holds) // 2]
            neg_pct = sum(1 for h in holds if h < 0) / len(holds)
            tight_pct = sum(1 for h in holds if 0 <= h < 2) / len(holds)
            edge_pct = sum(1 for h in holds if h >= 2) / len(holds)
            print(f"    {SPORT_SHORT.get(sport, sport):6s}  n={len(holds):4d}  avg={avg:+.1f}%  med={median:+.1f}%  neg={neg_pct:.0%}  tight={tight_pct:.0%}  edge+={edge_pct:.0%}")

    print("\nDone.")


if __name__ == "__main__":
    run()
