"""Analyze impact of threshold changes on PD signal hit rate.

Tags each graded Pinnacle Divergence signal as:
  - "legacy"   — would fire under old global thresholds (3% ML, 1.0 spread/totals)
  - "new_only" — only fires because of lowered sport-specific thresholds

Then breaks down hit rates by:
  1. Legacy vs new_only
  2. Sport × market
  3. Strength bucket
  4. UTC hour
"""

import json
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))

# Old thresholds (before sport-specific overrides)
OLD_ML_THRESHOLD = 0.03        # 3% implied probability
OLD_SPREAD_THRESHOLD = 1.0     # 1.0 points
OLD_TOTALS_THRESHOLD = 1.0     # same as spread (no separate totals threshold existed)

STRENGTH_BUCKETS = [
    (0.0, 0.33, "0.00–0.33"),
    (0.33, 0.50, "0.33–0.50"),
    (0.50, 0.67, "0.50–0.67"),
    (0.67, 1.01, "0.67–1.00"),
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
    print("  ERROR: Could not acquire DB lock after 10 attempts.")
    raise SystemExit(1)


def classify_signal(row):
    """Tag a signal as 'legacy' or 'new_only' based on old thresholds."""
    market = row["market_key"]
    details_raw = row["details_json"]
    if not details_raw:
        return "unknown"
    try:
        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
    except (json.JSONDecodeError, TypeError):
        return "unknown"

    delta = details.get("delta")
    if delta is None:
        return "unknown"

    if market == "h2h":
        return "legacy" if delta >= OLD_ML_THRESHOLD else "new_only"
    elif market == "spreads":
        return "legacy" if delta >= OLD_SPREAD_THRESHOLD else "new_only"
    elif market == "totals":
        return "legacy" if delta >= OLD_TOTALS_THRESHOLD else "new_only"
    return "unknown"


def fmt_rate(wins, losses, pushes):
    decided = wins + losses
    if decided == 0:
        return f"  {wins}W-{losses}L-{pushes}P  (no decided)"
    rate = wins / decided
    return f"  {wins}W-{losses}L-{pushes}P  ({rate:.0%})"


def print_section(title):
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def run():
    conn = connect()

    # Fetch all graded PD signals
    cur = conn.execute("""
        SELECT event_id, sport_key, signal_type, market_key, outcome_name,
               signal_strength, signal_at, result, details_json
        FROM signal_results
        WHERE signal_type = 'pinnacle_divergence'
          AND result IS NOT NULL
        ORDER BY signal_at
    """)
    rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        print("No graded Pinnacle Divergence signals found.")
        conn.close()
        return

    total = len(rows)
    first_dt = datetime.fromisoformat(rows[0]["signal_at"]).astimezone(MST)
    last_dt = datetime.fromisoformat(rows[-1]["signal_at"]).astimezone(MST)
    print(f"Analyzing {total} graded PD signals")
    print(f"Range: {first_dt.strftime('%m/%d %I:%M %p')} — {last_dt.strftime('%m/%d %I:%M %p')} MST")

    # Classify each signal
    for row in rows:
        row["config_class"] = classify_signal(row)

    # ── 1. Legacy vs New_Only ────────────────────────────────
    print_section("1. LEGACY vs NEW-ONLY HIT RATE")
    print("  Legacy  = would fire under old global thresholds")
    print("  New     = only fires with sport-specific overrides")
    print()

    by_class = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for row in rows:
        by_class[row["config_class"]][row["result"]] += 1

    for cls in ["legacy", "new_only", "unknown"]:
        if cls not in by_class:
            continue
        d = by_class[cls]
        n = d["won"] + d["lost"] + d["push"]
        label = {"legacy": "Legacy", "new_only": "New only", "unknown": "Unknown"}[cls]
        print(f"  {label:10s} (n={n:3d}){fmt_rate(d['won'], d['lost'], d['push'])}")

    # ── 2. Sport × Market ────────────────────────────────────
    print_section("2. SPORT × MARKET HIT RATE")

    by_sport_market = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for row in rows:
        key = f"{row['sport_key']}:{row['market_key']}"
        by_sport_market[key][row["result"]] += 1

    for key in sorted(by_sport_market.keys()):
        d = by_sport_market[key]
        n = d["won"] + d["lost"] + d["push"]
        print(f"  {key:40s} (n={n:3d}){fmt_rate(d['won'], d['lost'], d['push'])}")

    # ── 2b. Sport × Market × Config Class ────────────────────
    print_section("2b. SPORT × MARKET — LEGACY vs NEW-ONLY")

    by_sport_market_class = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for row in rows:
        key = f"{row['sport_key']}:{row['market_key']}:{row['config_class']}"
        by_sport_market_class[key][row["result"]] += 1

    for key in sorted(by_sport_market_class.keys()):
        d = by_sport_market_class[key]
        n = d["won"] + d["lost"] + d["push"]
        print(f"  {key:50s} (n={n:3d}){fmt_rate(d['won'], d['lost'], d['push'])}")

    # ── 3. Strength Buckets ──────────────────────────────────
    print_section("3. STRENGTH BUCKET HIT RATE")

    by_strength = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for row in rows:
        strength = row["signal_strength"]
        for lo, hi, label in STRENGTH_BUCKETS:
            if lo <= strength < hi:
                by_strength[label][row["result"]] += 1
                break

    for _, _, label in STRENGTH_BUCKETS:
        if label not in by_strength:
            continue
        d = by_strength[label]
        n = d["won"] + d["lost"] + d["push"]
        print(f"  {label:15s} (n={n:3d}){fmt_rate(d['won'], d['lost'], d['push'])}")

    # ── 3b. Strength Buckets by Config Class ─────────────────
    print_section("3b. STRENGTH BUCKET — LEGACY vs NEW-ONLY")

    by_strength_class = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for row in rows:
        strength = row["signal_strength"]
        for lo, hi, label in STRENGTH_BUCKETS:
            if lo <= strength < hi:
                key = f"{label}:{row['config_class']}"
                by_strength_class[key][row["result"]] += 1
                break

    for _, _, label in STRENGTH_BUCKETS:
        for cls in ["legacy", "new_only"]:
            key = f"{label}:{cls}"
            if key not in by_strength_class:
                continue
            d = by_strength_class[key]
            n = d["won"] + d["lost"] + d["push"]
            cls_label = "legacy" if cls == "legacy" else "new   "
            print(f"  {label:15s} {cls_label}  (n={n:3d}){fmt_rate(d['won'], d['lost'], d['push'])}")

    # ── 4. UTC Hour ──────────────────────────────────────────
    print_section("4. UTC HOUR HIT RATE")
    print("  (Hours with no signals omitted)")
    print()

    by_hour = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for row in rows:
        try:
            dt = datetime.fromisoformat(row["signal_at"])
            by_hour[dt.hour][row["result"]] += 1
        except (ValueError, TypeError):
            pass

    for hour in range(24):
        if hour not in by_hour:
            continue
        d = by_hour[hour]
        n = d["won"] + d["lost"] + d["push"]
        mst_hour = (hour - 7) % 24
        mst_ampm = "AM" if mst_hour < 12 else "PM"
        mst_display = mst_hour % 12 or 12
        print(f"  {hour:02d} UTC ({mst_display:2d} {mst_ampm} MST)  (n={n:3d}){fmt_rate(d['won'], d['lost'], d['push'])}")

    # ── 5. Delta Distribution for New-Only ───────────────────
    print_section("5. NEW-ONLY SIGNAL DELTAS (detail)")
    print("  Shows what deltas the new thresholds are catching")
    print()

    new_only = [r for r in rows if r["config_class"] == "new_only"]
    for row in sorted(new_only, key=lambda r: r["signal_at"]):
        details = json.loads(row["details_json"]) if row["details_json"] else {}
        delta = details.get("delta", "?")
        us_book = details.get("us_book", "?")
        dt = datetime.fromisoformat(row["signal_at"]).astimezone(MST)
        result_emoji = {"won": "W", "lost": "L", "push": "P"}.get(row["result"], "?")
        sport_short = row["sport_key"].split("_")[-1].upper()
        market = row["market_key"]

        if market == "h2h":
            delta_str = f"{delta:.1%}" if isinstance(delta, float) else str(delta)
        else:
            delta_str = f"{delta:+.1f}" if isinstance(delta, (int, float)) else str(delta)

        print(
            f"  {result_emoji}  {dt.strftime('%m/%d %I:%M%p'):14s}"
            f"  {sport_short:5s} {market:8s}"
            f"  delta={delta_str:>7s}  str={row['signal_strength']:.2f}"
            f"  {us_book}"
        )

    # ── 6. Summary ───────────────────────────────────────────
    print_section("6. SUMMARY")

    legacy_d = by_class.get("legacy", {"won": 0, "lost": 0, "push": 0})
    new_d = by_class.get("new_only", {"won": 0, "lost": 0, "push": 0})
    legacy_decided = legacy_d["won"] + legacy_d["lost"]
    new_decided = new_d["won"] + new_d["lost"]

    print(f"  Total graded PD signals: {total}")
    print(f"  Legacy signals:  {legacy_d['won'] + legacy_d['lost'] + legacy_d['push']}")
    print(f"  New-only signals: {new_d['won'] + new_d['lost'] + new_d['push']}")
    print()
    if legacy_decided > 0:
        print(f"  Legacy hit rate:   {legacy_d['won']}/{legacy_decided} = {legacy_d['won']/legacy_decided:.0%}")
    if new_decided > 0:
        print(f"  New-only hit rate: {new_d['won']}/{new_decided} = {new_d['won']/new_decided:.0%}")
    if legacy_decided > 0 and new_decided > 0:
        legacy_rate = legacy_d["won"] / legacy_decided
        new_rate = new_d["won"] / new_decided
        diff = new_rate - legacy_rate
        print(f"  Difference:        {diff:+.0%} ({'new_only better' if diff > 0 else 'legacy better' if diff < 0 else 'same'})")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    run()
