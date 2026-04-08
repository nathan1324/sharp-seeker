"""Diagnose why YTD units match April-only units on the recap card.

Usage:
    docker compose exec sharp-seeker python /app/scripts/diagnose_ytd.py
"""

import sqlite3
from datetime import datetime, timezone

DB = "/app/data/sharp_seeker.db"

now = datetime(2026, 4, 8, 11, 45, 0, tzinfo=timezone.utc)
ytd_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

print(f"ytd_start param: {ytd_start.isoformat()}")
print(f"month_start param: {month_start.isoformat()}")
print()

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# 1. Total free play rows
total = conn.execute("SELECT COUNT(*) FROM sent_alerts WHERE is_free_play = 1").fetchone()[0]
print(f"Total free play rows (is_free_play=1): {total}")

# 2. Free plays by month
rows = conn.execute("""
    SELECT substr(sent_at, 1, 7) AS month, COUNT(*) AS cnt
    FROM sent_alerts WHERE is_free_play = 1
    GROUP BY month ORDER BY month
""").fetchall()
print("\nFree plays by month:")
for r in rows:
    print(f"  {r['month']}: {r['cnt']}")

# 3. Free plays with graded results by month
rows = conn.execute("""
    SELECT substr(sa.sent_at, 1, 7) AS month,
           COUNT(*) AS total,
           SUM(CASE WHEN sr.result IS NOT NULL THEN 1 ELSE 0 END) AS graded
    FROM sent_alerts sa
    LEFT JOIN signal_results sr
      ON sa.event_id = sr.event_id
     AND sa.alert_type = sr.signal_type
     AND sa.market_key = sr.market_key
     AND sa.outcome_name = sr.outcome_name
    WHERE sa.is_free_play = 1
    GROUP BY month ORDER BY month
""").fetchall()
print("\nFree plays with graded results by month:")
for r in rows:
    print(f"  {r['month']}: {r['total']} total, {r['graded']} graded")

# 4. Sample sent_at values (first and last)
first = conn.execute("SELECT sent_at FROM sent_alerts WHERE is_free_play = 1 ORDER BY sent_at ASC LIMIT 3").fetchall()
last = conn.execute("SELECT sent_at FROM sent_alerts WHERE is_free_play = 1 ORDER BY sent_at DESC LIMIT 3").fetchall()
print("\nFirst 3 free play sent_at values:")
for r in first:
    print(f"  {r['sent_at']}")
print("Last 3 free play sent_at values:")
for r in last:
    print(f"  {r['sent_at']}")

# 5. Test the actual YTD query
ytd_count = conn.execute(
    "SELECT COUNT(*) FROM sent_alerts WHERE is_free_play = 1 AND sent_at >= ?",
    (ytd_start.isoformat(),)
).fetchone()[0]
month_count = conn.execute(
    "SELECT COUNT(*) FROM sent_alerts WHERE is_free_play = 1 AND sent_at >= ?",
    (month_start.isoformat(),)
).fetchone()[0]
print(f"\nYTD query (sent_at >= {ytd_start.isoformat()}): {ytd_count} rows")
print(f"Month query (sent_at >= {month_start.isoformat()}): {month_count} rows")

if ytd_count == month_count:
    print("\n*** YTD and MONTH counts are IDENTICAL -- confirms the bug ***")
    print("Checking if pre-April free plays exist but fail the >= comparison...")
    pre_april = conn.execute(
        "SELECT COUNT(*) FROM sent_alerts WHERE is_free_play = 1 AND sent_at < ?",
        (month_start.isoformat(),)
    ).fetchone()[0]
    print(f"Free plays before April 1: {pre_april}")
    if pre_april > 0:
        samples = conn.execute(
            "SELECT sent_at FROM sent_alerts WHERE is_free_play = 1 AND sent_at < ? ORDER BY sent_at DESC LIMIT 5",
            (month_start.isoformat(),)
        ).fetchall()
        print("Sample pre-April sent_at values:")
        for r in samples:
            val = r['sent_at']
            passes = val >= ytd_start.isoformat()
            print(f"  {val}  (>= ytd_start? {passes})")

conn.close()
