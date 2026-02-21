"""Quick script to check API credit usage and project monthly costs."""

import sqlite3
import sys

db_path = sys.argv[1] if len(sys.argv) > 1 else "/app/data/sharp_seeker.db"
db = sqlite3.connect(db_path)
db.row_factory = sqlite3.Row

# Check tables exist
tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
print(f"Tables: {tables}")

if "api_usage" not in tables:
    print("No api_usage table found.")
    sys.exit(1)

rows = db.execute("""
    SELECT
        date(timestamp) as day,
        COUNT(*) as polls,
        SUM(credits_used) as credits_used,
        MIN(credits_remaining) as credits_remaining
    FROM api_usage
    WHERE timestamp >= datetime('now', '-2 days')
    GROUP BY date(timestamp)
    ORDER BY day
""").fetchall()

total_credits = 0
total_days = 0
for r in rows:
    print(f"{r['day']}: {r['polls']} polls, {r['credits_used']} credits used, {r['credits_remaining']} remaining")
    total_credits += r["credits_used"]
    total_days += 1

if total_days > 0:
    daily_avg = total_credits / total_days
    print(f"\n--- Projections ---")
    print(f"Daily avg: {daily_avg:.0f} credits")
    print(f"Weekly:    {daily_avg * 7:.0f} credits")
    print(f"Monthly:   {daily_avg * 30:.0f} credits")
    print(f"Budget:    500 credits/month")
    print(f"Headroom:  {500 - daily_avg * 30:.0f} credits")
else:
    print("No usage data in last 2 days.")
