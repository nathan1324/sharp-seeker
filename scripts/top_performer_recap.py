"""Recap performance of signals that would receive the Top Performer badge.

Checks both SIGNAL_BEST_COMBOS (type:sport:market) and SIGNAL_BEST_HOURS
(type:hour(MST)) against graded signal_results.

Usage:
    docker compose exec sharp-seeker python /app/scripts/top_performer_recap.py

Optional: pass a date to only show results since that date:
    docker compose exec sharp-seeker python /app/scripts/top_performer_recap.py 2026-03-09
"""

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

DB = os.getenv("DB_PATH", "/app/data/sharp_seeker.db")
MST = ZoneInfo("America/Phoenix")

MARKET_NAMES = {"spreads": "Spread", "totals": "Total", "h2h": "Moneyline"}
RESULT_EMOJI = {"won": "W", "lost": "L", "push": "P"}


def compute_units(price, result):
    """Compute units won/lost assuming bet-to-win-1u."""
    if result == "push" or price is None:
        return 0.0
    if price < 0:
        risk = abs(price) / 100.0
    else:
        risk = 100.0 / price if price > 0 else 1.0
    if result == "won":
        return 1.0
    elif result == "lost":
        return -risk
    return 0.0


def _get_price(row):
    """Extract best book price from details_json."""
    details_raw = row.get("details_json")
    if not details_raw:
        return None
    try:
        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        vb = details.get("value_books", [])
        return vb[0].get("price") if vb else None
    except (json.JSONDecodeError, TypeError):
        return None

# Parse configs from env (same format as the app)
BEST_COMBOS = set(json.loads(os.getenv("SIGNAL_BEST_COMBOS", "[]")))
BEST_HOURS = {
    k: set(v)
    for k, v in json.loads(os.getenv("SIGNAL_BEST_HOURS", "{}")).items()
}


def is_top_performer(signal_type, sport_key, market_key, signal_at):
    """Check if a signal would have received the Top Performer badge."""
    # Check best combos
    combo_key = f"{signal_type}:{sport_key}:{market_key}"
    if combo_key in BEST_COMBOS:
        return True, "combo"

    # Check best hours (MST hour at signal time)
    hours = BEST_HOURS.get(signal_type)
    if hours and signal_at:
        try:
            dt = datetime.fromisoformat(signal_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            mst_hour = dt.astimezone(MST).hour
            if mst_hour in hours:
                return True, "hour"
        except (ValueError, TypeError):
            pass

    return False, None


def main():
    since = sys.argv[1] if len(sys.argv) > 1 else None

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = """
        SELECT event_id, sport_key, signal_type, market_key, outcome_name,
               signal_direction, signal_strength, signal_at, result,
               details_json
        FROM signal_results
        WHERE result IN ('won', 'lost', 'push')
    """
    params = []
    if since:
        query += " AND signal_at >= ?"
        params.append(since)
    query += " ORDER BY signal_at ASC"

    rows = cur.execute(query, params).fetchall()

    tagged = []
    untagged = []

    for r in rows:
        row = dict(r)
        is_tp, reason = is_top_performer(
            row["signal_type"], row["sport_key"],
            row["market_key"], row["signal_at"],
        )
        if is_tp:
            row["_tp_reason"] = reason
            tagged.append(row)
        else:
            untagged.append(row)

    # --- Tagged signals ---
    tagged_w = sum(1 for r in tagged if r["result"] == "won")
    tagged_l = sum(1 for r in tagged if r["result"] == "lost")
    tagged_p = sum(1 for r in tagged if r["result"] == "push")
    tagged_u = sum(compute_units(_get_price(r), r["result"]) for r in tagged)
    tagged_resolved = tagged_w + tagged_l
    tagged_pct = tagged_w / tagged_resolved * 100 if tagged_resolved else 0

    # --- Untagged signals ---
    untagged_w = sum(1 for r in untagged if r["result"] == "won")
    untagged_l = sum(1 for r in untagged if r["result"] == "lost")
    untagged_p = sum(1 for r in untagged if r["result"] == "push")
    untagged_u = sum(compute_units(_get_price(r), r["result"]) for r in untagged)
    untagged_resolved = untagged_w + untagged_l
    untagged_pct = untagged_w / untagged_resolved * 100 if untagged_resolved else 0

    # Sub-group units
    combo_rows = [r for r in tagged if r["_tp_reason"] == "combo"]
    hour_rows = [r for r in tagged if r["_tp_reason"] == "hour"]
    combo_u = sum(compute_units(_get_price(r), r["result"]) for r in combo_rows)
    hour_u = sum(compute_units(_get_price(r), r["result"]) for r in hour_rows)

    since_label = f" (since {since})" if since else ""
    print(f"=== TOP PERFORMER BADGE RECAP{since_label} ===")
    print(f"Config: {len(BEST_COMBOS)} best combos, {sum(len(v) for v in BEST_HOURS.values())} best hour slots")
    print()

    print(f"{'Category':<25s} {'W':>4s} {'L':>4s} {'P':>4s} {'Total':>6s} {'Win%':>6s} {'Units':>8s}")
    print("-" * 65)
    print(f"{'⭐ Top Performer':<25s} {tagged_w:>4d} {tagged_l:>4d} {tagged_p:>4d} {tagged_resolved:>6d} {tagged_pct:>5.1f}% {tagged_u:>+7.1f}u")
    cw = sum(1 for r in combo_rows if r["result"] == "won")
    cl = sum(1 for r in combo_rows if r["result"] == "lost")
    cp = sum(1 for r in combo_rows if r["result"] == "push")
    print(f"{'   (combo match)':<25s} {cw:>4d} {cl:>4d} {cp:>4d} {'':>6s} {'':>6s} {combo_u:>+7.1f}u")
    hw = sum(1 for r in hour_rows if r["result"] == "won")
    hl = sum(1 for r in hour_rows if r["result"] == "lost")
    hp = sum(1 for r in hour_rows if r["result"] == "push")
    print(f"{'   (hour match)':<25s} {hw:>4d} {hl:>4d} {hp:>4d} {'':>6s} {'':>6s} {hour_u:>+7.1f}u")
    print(f"{'No badge':<25s} {untagged_w:>4d} {untagged_l:>4d} {untagged_p:>4d} {untagged_resolved:>6d} {untagged_pct:>5.1f}% {untagged_u:>+7.1f}u")
    print()

    # Breakdown by combo key
    print("=== BY BEST COMBO ===")
    combo_stats = {}
    for r in tagged:
        if r["_tp_reason"] != "combo":
            continue
        key = f"{r['signal_type']}:{r['sport_key']}:{r['market_key']}"
        if key not in combo_stats:
            combo_stats[key] = {"won": 0, "lost": 0, "push": 0, "units": 0.0}
        combo_stats[key][r["result"]] += 1
        combo_stats[key]["units"] += compute_units(_get_price(r), r["result"])

    if combo_stats:
        for key in sorted(combo_stats):
            s = combo_stats[key]
            w, l = s["won"], s["lost"]
            resolved = w + l
            pct = f"{w/resolved*100:.0f}%" if resolved else "N/A"
            print(f"  {key:50s} {w}-{l} ({pct})  {s['units']:+.1f}u")
    else:
        print("  (no resolved signals match best combos)")

    # Breakdown by best hour
    print(f"\n=== BY BEST HOUR (MST) ===")
    hour_stats = {}
    for r in tagged:
        if r["_tp_reason"] != "hour":
            continue
        dt = datetime.fromisoformat(r["signal_at"])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        mst_hour = dt.astimezone(MST).hour
        key = f"{r['signal_type']}:hour_{mst_hour}"
        if key not in hour_stats:
            hour_stats[key] = {"won": 0, "lost": 0, "push": 0, "units": 0.0}
        hour_stats[key][r["result"]] += 1
        hour_stats[key]["units"] += compute_units(_get_price(r), r["result"])

    if hour_stats:
        for key in sorted(hour_stats):
            s = hour_stats[key]
            w, l = s["won"], s["lost"]
            resolved = w + l
            pct = f"{w/resolved*100:.0f}%" if resolved else "N/A"
            print(f"  {key:50s} {w}-{l} ({pct})  {s['units']:+.1f}u")
    else:
        print("  (no resolved signals match best hours)")

    # Breakdown by signal type
    print(f"\n=== BY SIGNAL TYPE ===")
    for label, group in [("⭐ Tagged", tagged), ("No badge", untagged)]:
        type_stats = {}
        for r in group:
            st = r["signal_type"]
            if st not in type_stats:
                type_stats[st] = {"won": 0, "lost": 0, "units": 0.0}
            if r["result"] in ("won", "lost"):
                type_stats[st][r["result"]] += 1
                type_stats[st]["units"] += compute_units(_get_price(r), r["result"])
        print(f"  {label}:")
        for st in sorted(type_stats):
            s = type_stats[st]
            w, l = s["won"], s["lost"]
            resolved = w + l
            pct = f"{w/resolved*100:.0f}%" if resolved else "N/A"
            print(f"    {st:40s} {w}-{l} ({pct})  {s['units']:+.1f}u")

    # Breakdown by sport
    print(f"\n=== BY SPORT ===")
    for label, group in [("⭐ Tagged", tagged), ("No badge", untagged)]:
        sport_stats = {}
        for r in group:
            sp = r["sport_key"] or "unknown"
            if sp not in sport_stats:
                sport_stats[sp] = {"won": 0, "lost": 0, "units": 0.0}
            if r["result"] in ("won", "lost"):
                sport_stats[sp][r["result"]] += 1
                sport_stats[sp]["units"] += compute_units(_get_price(r), r["result"])
        print(f"  {label}:")
        for sp in sorted(sport_stats):
            s = sport_stats[sp]
            w, l = s["won"], s["lost"]
            resolved = w + l
            pct = f"{w/resolved*100:.0f}%" if resolved else "N/A"
            print(f"    {sp:40s} {w}-{l} ({pct})  {s['units']:+.1f}u")

    # Last 15 tagged results
    print(f"\n=== LAST 15 TAGGED RESULTS ===")
    for r in tagged[-15:]:
        tag = RESULT_EMOJI.get(r["result"], "?")
        price = _get_price(r)
        u = compute_units(price, r["result"])
        unit_str = f"{u:+.2f}u" if r["result"] in ("won", "lost") else "     "
        market_name = MARKET_NAMES.get(r["market_key"], r["market_key"])
        dt = datetime.fromisoformat(r["signal_at"])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        mst = dt.astimezone(MST)
        date_str = mst.strftime("%b %d %I:%M%p")
        reason = r["_tp_reason"]
        print(
            f"  [{tag}] {unit_str:7s} {date_str:16s} | {r['signal_type']:25s} | "
            f"{r['sport_key']:25s} | {market_name:10s} | "
            f"{r['outcome_name']:15s} | ({reason})"
        )

    conn.close()


if __name__ == "__main__":
    main()
