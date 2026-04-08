"""Diagnose why YTD units match April-only units on the recap card.

Usage:
    docker compose exec sharp-seeker python /app/scripts/diagnose_ytd.py
"""

import json
import sqlite3
from datetime import datetime, timezone

DB = "/app/data/sharp_seeker.db"

now = datetime(2026, 4, 8, 11, 45, 0, tzinfo=timezone.utc)
ytd_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

QUERY = """
    SELECT sa.event_id, sa.market_key, sa.outcome_name, sa.sent_at,
           sa.details_json,
           sr.result, sr.signal_strength
    FROM sent_alerts sa
    LEFT JOIN signal_results sr
      ON sa.event_id = sr.event_id
     AND sa.alert_type = sr.signal_type
     AND sa.market_key = sr.market_key
     AND sa.outcome_name = sr.outcome_name
    WHERE sa.is_free_play = 1
      AND sa.sent_at >= ?
    ORDER BY sa.sent_at ASC
"""


def compute_risk(price):
    if price < 0:
        return abs(price) / 100.0
    elif price > 0:
        return 100.0 / price
    return 1.0


def tally(rows):
    """Replicate the card_generator _tally method (before 2U fix)."""
    wins = losses = 0
    units = 0.0
    for row in rows:
        r = dict(row)
        result = r.get("result")
        if result == "won":
            wins += 1
            units += 1.0
        elif result == "lost":
            losses += 1
            details_raw = r.get("details_json")
            if details_raw:
                details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
                vb = details.get("value_books", [])
                price = vb[0].get("price") if vb else None
                units -= compute_risk(price) if price else 1.0
            else:
                units -= 1.0
    return wins, losses, round(units, 2)


conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Run the exact same queries as _get_stats
ytd_rows = conn.execute(QUERY, (ytd_start.isoformat(),)).fetchall()
month_rows = conn.execute(QUERY, (month_start.isoformat(),)).fetchall()

ytd_resolved = [r for r in ytd_rows if dict(r).get("result") is not None]
month_resolved = [r for r in month_rows if dict(r).get("result") is not None]

ytd_w, ytd_l, ytd_u = tally(ytd_resolved)
m_w, m_l, m_u = tally(month_resolved)

print("=== REPLICATING CARD GENERATOR LOGIC ===")
print(f"YTD query rows: {len(ytd_rows)}, resolved: {len(ytd_resolved)}")
print(f"Month query rows: {len(month_rows)}, resolved: {len(month_resolved)}")
print(f"YTD tally:   {ytd_w}-{ytd_l}, {ytd_u:+.2f}u")
print(f"Month tally: {m_w}-{m_l}, {m_u:+.2f}u")
print(f"Match? {ytd_u == m_u}")

# Break down YTD by month to find where units come from
print("\n=== YTD TALLY BY MONTH ===")
by_month = {}
for row in ytd_resolved:
    r = dict(row)
    month = r["sent_at"][:7]
    if month not in by_month:
        by_month[month] = []
    by_month[month].append(row)

for month in sorted(by_month):
    w, l, u = tally(by_month[month])
    print(f"  {month}: {w}-{l}, {u:+.2f}u ({len(by_month[month])} resolved)")

# Check for duplicate joins (sent_alert matching multiple signal_results)
print("\n=== DUPLICATE JOIN CHECK ===")
dup_check = conn.execute("""
    SELECT sa.id, sa.event_id, sa.outcome_name, COUNT(*) as match_count
    FROM sent_alerts sa
    LEFT JOIN signal_results sr
      ON sa.event_id = sr.event_id
     AND sa.alert_type = sr.signal_type
     AND sa.market_key = sr.market_key
     AND sa.outcome_name = sr.outcome_name
    WHERE sa.is_free_play = 1
    GROUP BY sa.id
    HAVING match_count > 1
    ORDER BY match_count DESC
    LIMIT 10
""").fetchall()
if dup_check:
    print(f"Found {len(dup_check)} free plays with multiple signal_results matches:")
    for r in dup_check:
        print(f"  sa.id={r['id']} event={r['event_id'][:30]} outcome={r['outcome_name']} matches={r['match_count']}")
else:
    print("No duplicate joins found")

# Check for NULL results in pre-April data
print("\n=== NULL RESULTS CHECK ===")
pre_april_null = conn.execute("""
    SELECT sa.event_id, sa.outcome_name, sa.sent_at, sr.result
    FROM sent_alerts sa
    LEFT JOIN signal_results sr
      ON sa.event_id = sr.event_id
     AND sa.alert_type = sr.signal_type
     AND sa.market_key = sr.market_key
     AND sa.outcome_name = sr.outcome_name
    WHERE sa.is_free_play = 1
      AND sa.sent_at < ?
      AND sr.result IS NULL
""", (month_start.isoformat(),)).fetchall()
print(f"Pre-April free plays with NULL result: {len(pre_april_null)}")
for r in pre_april_null[:5]:
    print(f"  {r['sent_at'][:16]} {r['outcome_name']} result={r['result']}")

conn.close()
