"""Quick script to check API credit usage and project monthly costs.

Note: The Odds API x-requests-used header is CUMULATIVE for the billing
period, so we calculate actual daily usage from the change in
credits_remaining (first poll vs last poll each day).
"""

import sqlite3
import sys

db_path = sys.argv[1] if len(sys.argv) > 1 else "/app/data/sharp_seeker.db"
db = sqlite3.connect(db_path)
db.row_factory = sqlite3.Row

tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
if "api_usage" not in tables:
    print("No api_usage table found.")
    sys.exit(1)

# Get first and last credits_remaining per day to compute actual usage
rows = db.execute("""
    SELECT
        date(timestamp) as day,
        COUNT(*) as polls,
        MAX(credits_remaining) as day_start_remaining,
        MIN(credits_remaining) as day_end_remaining
    FROM api_usage
    WHERE timestamp >= datetime('now', '-7 days')
    GROUP BY date(timestamp)
    ORDER BY day
""").fetchall()

# Current state
latest = db.execute(
    "SELECT credits_used, credits_remaining FROM api_usage ORDER BY id DESC LIMIT 1"
).fetchone()

print("=== Daily Usage (last 7 days) ===")
total_credits = 0
total_days = 0
for r in rows:
    daily_usage = r["day_start_remaining"] - r["day_end_remaining"]
    print(f"  {r['day']}: {r['polls']} polls, {daily_usage} credits used")
    total_credits += daily_usage
    total_days += 1

if latest:
    print(f"\n=== Current Status ===")
    print(f"  Credits used (billing period): {latest['credits_used']}")
    print(f"  Credits remaining:             {latest['credits_remaining']}")
    total_pool = latest["credits_used"] + latest["credits_remaining"]
    print(f"  Total credit pool:             {total_pool}")

if total_days > 0:
    daily_avg = total_credits / total_days
    total_pool = (latest["credits_used"] + latest["credits_remaining"]) if latest else 500
    print(f"\n=== Projections ===")
    print(f"  Daily avg:  {daily_avg:.0f} credits")
    print(f"  Weekly:     {daily_avg * 7:.0f} credits")
    print(f"  Monthly:    {daily_avg * 30:.0f} credits")
    print(f"  Pool:       {total_pool} credits/month")
    headroom = total_pool - daily_avg * 30
    print(f"  Headroom:   {headroom:+.0f} credits")
    if daily_avg > 0 and latest:
        days_left = latest["credits_remaining"] / daily_avg
        print(f"  Days left:  {days_left:.1f} days at current rate")
else:
    print("No usage data found.")
