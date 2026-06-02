"""Analyze PD signals by Pinnacle's recent line-direction annotation.

Backend measurement read-out (see signaling-system.md, 2026-06-01 entry):
each Pinnacle Divergence h2h/spread signal is tagged with whether the sharp
line was moving TOWARD the flagged side (sharp money backing it) or AGAINST
it (sharp money fading it) over the recent window. This script buckets
resolved PD signals by that tag and reports win% / units, so we can see
whether "against" (catching a move mid-flight) actually underperforms before
deciding to suppress it.

Usage (on the server):
    docker compose exec sharp-seeker python /app/scripts/pd_direction_analysis.py
Optional args: [db_path] [since_iso]   e.g. ... pd_direction_analysis.py "" 2026-06-01
"""

import json
import sqlite3
import sys

DB_PATH = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else "/app/data/sharp_seeker.db"
SINCE = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else None


def american_to_profit(price):
    """Profit per 1u staked on a win at American odds."""
    if price is None:
        return 1.0  # assume even money if unknown
    if price > 0:
        return price / 100.0
    return 100.0 / abs(price)


def bet_price(details_raw):
    try:
        details = json.loads(details_raw) if details_raw else {}
        vbs = details.get("value_books", [])
        if vbs:
            return vbs[0].get("price")
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    where = (
        "signal_type = 'pinnacle_divergence' "
        "AND market_key IN ('h2h', 'spreads') "
        "AND result IS NOT NULL"
    )
    params = []
    if SINCE:
        where += " AND signal_at >= ?"
        params.append(SINCE)

    sql = (
        "SELECT market_key, result, signal_strength, details_json, "
        "json_extract(details_json, '$.pinnacle_recent_direction') AS direction "
        "FROM signal_results WHERE " + where
    )
    rows = [dict(r) for r in db.execute(sql, params).fetchall()]
    db.close()

    if not rows:
        print("No resolved PD h2h/spread signals found"
              + (f" since {SINCE}" if SINCE else "")
              + ". (Annotation only applies to signals graded after deploy.)")
        return

    # bucket -> {market -> stats}
    def blank():
        return {"won": 0, "lost": 0, "push": 0, "units": 0.0}

    buckets = {}
    for row in rows:
        direction = row.get("direction") or "unknown"
        market = row["market_key"]
        result = row["result"]
        key = (direction, market)
        stats = buckets.setdefault(key, blank())
        if result == "won":
            stats["won"] += 1
            stats["units"] += american_to_profit(bet_price(row.get("details_json")))
        elif result == "lost":
            stats["lost"] += 1
            stats["units"] -= 1.0
        else:
            stats["push"] += 1

    span = f" (since {SINCE})" if SINCE else " (all time)"
    print(f"PD direction analysis{span} — DB: {DB_PATH}")
    print(f"Resolved PD h2h/spread signals: {len(rows)}\n")

    header = "  {:<9} {:<8} {:>4} {:>4} {:>4} {:>6} {:>9} {:>8}".format(
        "direction", "market", "W", "L", "P", "win%", "units", "ROI%"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))

    for (direction, market) in sorted(buckets):
        s = buckets[(direction, market)]
        decided = s["won"] + s["lost"]
        win_pct = (100.0 * s["won"] / decided) if decided else 0.0
        plays = s["won"] + s["lost"] + s["push"]
        roi = (100.0 * s["units"] / plays) if plays else 0.0
        print("  {:<9} {:<8} {:>4} {:>4} {:>4} {:>5.1f}% {:>+9.2f} {:>+7.1f}%".format(
            direction, market, s["won"], s["lost"], s["push"], win_pct, s["units"], roi
        ))

    # Direction roll-up across both markets
    print("\n  Roll-up by direction (h2h + spreads):")
    roll = {}
    for (direction, _market), s in buckets.items():
        r = roll.setdefault(direction, blank())
        for k in r:
            r[k] += s[k]
    for direction in sorted(roll):
        s = roll[direction]
        decided = s["won"] + s["lost"]
        win_pct = (100.0 * s["won"] / decided) if decided else 0.0
        plays = s["won"] + s["lost"] + s["push"]
        roi = (100.0 * s["units"] / plays) if plays else 0.0
        print("    {:<9} {:>4}-{:<4} ({} push)  win {:>5.1f}%  units {:>+8.2f}  ROI {:>+6.1f}%".format(
            direction, s["won"], s["lost"], s["push"], win_pct, s["units"], roi
        ))


if __name__ == "__main__":
    main()
