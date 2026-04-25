"""Spread and ML records by signal type (last N days).

Usage:
    python /app/scripts/analyze_spread_ml_by_type.py            # default 21 days
    python /app/scripts/analyze_spread_ml_by_type.py 14         # last 14 days
    python /app/scripts/analyze_spread_ml_by_type.py 30         # last 30 days

Purpose: validate the "spreads must be Steam type only" X free-play policy by
comparing recent spread (and ML, for context) records across signal types.
Reports overall, per-sport, and free-play-only (sent_alerts.is_free_play=1)
slices.
"""

import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))

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

MARKET_LABELS = {"spreads": "Spread", "h2h": "ML"}
TARGET_MARKETS = ("spreads", "h2h")


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


def compute_units(price, result):
    if result == "push" or price is None:
        return 0.0
    if price < 0:
        risk = abs(price) / 100.0
    else:
        risk = 100.0 / price if price > 0 else 1.0
    if result == "won":
        return 1.0
    if result == "lost":
        return -risk
    return 0.0


def fmt(d):
    w, l, p, u = d["won"], d["lost"], d["push"], d["units"]
    n = w + l + p
    decided = w + l
    rate_str = f"{w / decided:.0%}" if decided else "--"
    sign = "+" if u >= 0 else ""
    return f"(n={n:4d})  {w:3d}W-{l:3d}L-{p:2d}P  ({rate_str})  [{sign}{u:.1f}u]"


def tally(rows, key_fn):
    buckets = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
    for row in rows:
        k = key_fn(row)
        if k is None:
            continue
        buckets[k][row["result"]] += 1
        buckets[k]["units"] += compute_units(row.get("best_price"), row["result"])
    return buckets


def section(title):
    print()
    print("=" * 78)
    print(f"  {title}")
    print("=" * 78)


def run():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 21
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    conn = connect()
    cur = conn.execute(
        """
        SELECT event_id, sport_key, signal_type, market_key, outcome_name,
               signal_strength, signal_at, result, details_json
        FROM signal_results
        WHERE result IS NOT NULL
          AND signal_at >= ?
          AND market_key IN ('spreads', 'h2h')
        ORDER BY signal_at
        """,
        (cutoff,),
    )
    rows = [dict(r) for r in cur.fetchall()]

    fp_cur = conn.execute(
        "SELECT event_id, market_key, outcome_name FROM sent_alerts WHERE is_free_play = 1"
    )
    free_play_keys = {
        (r["event_id"], r["market_key"], r["outcome_name"]) for r in fp_cur.fetchall()
    }
    conn.close()

    if not rows:
        print(f"No graded spread/ML signals in last {days} days.")
        return

    for row in rows:
        row["dt"] = datetime.fromisoformat(row["signal_at"])
        row["is_free_play"] = (
            row["event_id"], row["market_key"], row["outcome_name"]
        ) in free_play_keys
        row["best_price"] = None
        details_raw = row.get("details_json")
        if details_raw:
            try:
                details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                vb = details.get("value_books", [])
                if vb:
                    row["best_price"] = vb[0].get("price")
            except (json.JSONDecodeError, TypeError):
                pass

    first_mst = rows[0]["dt"].astimezone(MST)
    last_mst = rows[-1]["dt"].astimezone(MST)
    print(f"Spread + ML records — last {days} days")
    print(f"Range: {first_mst.strftime('%m/%d %I:%M %p')} - {last_mst.strftime('%m/%d %I:%M %p')} MST")
    print(f"Graded signals: {len(rows)}")

    # ── 1. By market × signal type (the headline) ──────────────────
    section("1. ALL SIGNALS — Market x Signal Type")
    by_mt = tally(rows, lambda r: (r["market_key"], r["signal_type"]))
    for mkt in TARGET_MARKETS:
        print(f"\n  {MARKET_LABELS[mkt]}:")
        type_keys = sorted({k[1] for k in by_mt if k[0] == mkt})
        # Sort by units desc within market
        type_keys.sort(key=lambda t: by_mt[(mkt, t)]["units"], reverse=True)
        for st in type_keys:
            label = SIGNAL_LABELS.get(st, st)
            print(f"    {label:12s} {fmt(by_mt[(mkt, st)])}")

    # ── 2. Free plays only (what actually got tweeted) ─────────────
    section("2. FREE PLAYS ONLY — Market x Signal Type")
    fp_rows = [r for r in rows if r["is_free_play"]]
    if fp_rows:
        by_fp = tally(fp_rows, lambda r: (r["market_key"], r["signal_type"]))
        for mkt in TARGET_MARKETS:
            mkt_keys = sorted({k[1] for k in by_fp if k[0] == mkt})
            if not mkt_keys:
                print(f"\n  {MARKET_LABELS[mkt]}: (no free plays)")
                continue
            print(f"\n  {MARKET_LABELS[mkt]}:")
            mkt_keys.sort(key=lambda t: by_fp[(mkt, t)]["units"], reverse=True)
            for st in mkt_keys:
                label = SIGNAL_LABELS.get(st, st)
                print(f"    {label:12s} {fmt(by_fp[(mkt, st)])}")
    else:
        print("\n  No free plays graded in window.")

    # ── 3. Per sport breakdown (all signals) ───────────────────────
    section("3. ALL SIGNALS — Sport x Market x Signal Type")
    by_smt = tally(rows, lambda r: (r["sport_key"], r["market_key"], r["signal_type"]))
    sports = sorted({k[0] for k in by_smt})
    for sport in sports:
        print(f"\n  {SPORT_SHORT.get(sport, sport)}")
        for mkt in TARGET_MARKETS:
            type_keys = sorted({k[2] for k in by_smt if k[0] == sport and k[1] == mkt})
            if not type_keys:
                continue
            type_keys.sort(key=lambda t: by_smt[(sport, mkt, t)]["units"], reverse=True)
            print(f"    {MARKET_LABELS[mkt]}:")
            for st in type_keys:
                label = SIGNAL_LABELS.get(st, st)
                print(f"      {label:12s} {fmt(by_smt[(sport, mkt, st)])}")

    # ── 4. Spread-only summary (the policy in question) ────────────
    section("4. POLICY CHECK — SPREADS ONLY: Steam vs non-Steam")
    spread_rows = [r for r in rows if r["market_key"] == "spreads"]
    by_steam = tally(
        spread_rows,
        lambda r: "Steam" if r["signal_type"] == "steam_move" else "Non-Steam",
    )
    for k in ("Steam", "Non-Steam"):
        if k in by_steam:
            print(f"  {k:12s} {fmt(by_steam[k])}")

    # Same split, free plays only
    spread_fp = [r for r in spread_rows if r["is_free_play"]]
    if spread_fp:
        print("\n  Free plays only:")
        by_steam_fp = tally(
            spread_fp,
            lambda r: "Steam" if r["signal_type"] == "steam_move" else "Non-Steam",
        )
        for k in ("Steam", "Non-Steam"):
            if k in by_steam_fp:
                print(f"    {k:12s} {fmt(by_steam_fp[k])}")
    else:
        print("\n  Free plays only: (none in window)")

    print()
    print("Done.")


if __name__ == "__main__":
    run()
