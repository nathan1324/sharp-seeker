"""Analyze API credit usage and project costs for adding NHL."""

import sqlite3
from datetime import datetime, timezone

DB_PATH = "/app/data/sharp_seeker.db"

db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row

# ── Current state ──────────────────────────────────────────────────
row = db.execute(
    "SELECT credits_used, credits_remaining FROM api_usage ORDER BY id DESC LIMIT 1"
).fetchone()
total_budget = row["credits_used"] + row["credits_remaining"]
used = row["credits_used"]
remaining = row["credits_remaining"]

# ── Date range of usage ───────────────────────────────────────────
first = db.execute("SELECT MIN(timestamp) AS t FROM api_usage").fetchone()["t"]
last = db.execute("SELECT MAX(timestamp) AS t FROM api_usage").fetchone()["t"]

first_dt = datetime.fromisoformat(first)
last_dt = datetime.fromisoformat(last)
now = datetime.now(timezone.utc)
days_elapsed = (last_dt - first_dt).total_seconds() / 86400

# ── Per-sport breakdown ───────────────────────────────────────────
rows = db.execute("""
    SELECT endpoint, COUNT(*) AS calls, SUM(credits_used) AS total_credits
    FROM api_usage
    GROUP BY endpoint
    ORDER BY total_credits DESC
""").fetchall()

# ── Daily averages ────────────────────────────────────────────────
daily_rows = db.execute("""
    SELECT DATE(timestamp) AS day, SUM(credits_used) AS daily_credits, COUNT(*) AS calls
    FROM api_usage
    GROUP BY DATE(timestamp)
    ORDER BY day DESC
    LIMIT 14
""").fetchall()

# ── Calls per sport per day (recent 7 days) ──────────────────────
per_sport_daily = db.execute("""
    SELECT endpoint, COUNT(*) AS calls, SUM(credits_used) AS credits
    FROM api_usage
    WHERE timestamp >= datetime('now', '-7 days')
    GROUP BY endpoint
""").fetchall()

db.close()

# ── Output ────────────────────────────────────────────────────────
print("=" * 60)
print("SHARP SEEKER — API CREDIT ANALYSIS")
print("=" * 60)

print(f"\nPlan budget:     {total_budget:,} credits/month")
print(f"Used this cycle: {used:,}")
print(f"Remaining:       {remaining:,}")
print(f"Usage period:    {first[:10]} → {last[:10]} ({days_elapsed:.1f} days)")

print(f"\n{'─' * 60}")
print("BREAKDOWN BY ENDPOINT (all time)")
print(f"{'─' * 60}")
print(f"{'Endpoint':<45} {'Calls':>6} {'Credits':>8}")
for r in rows:
    print(f"{r['endpoint']:<45} {r['calls']:>6} {r['total_credits']:>8}")

print(f"\n{'─' * 60}")
print("DAILY CREDIT USAGE (last 14 days)")
print(f"{'─' * 60}")
print(f"{'Date':<14} {'Credits':>8} {'API Calls':>10}")
for r in daily_rows:
    print(f"{r['day']:<14} {r['daily_credits']:>8} {r['calls']:>10}")

if days_elapsed > 0:
    # Use last 7 days for a more accurate burn rate
    recent_7 = [r for r in daily_rows if len(daily_rows) >= 7][:7]
    if recent_7:
        avg_daily = sum(r["daily_credits"] for r in recent_7) / len(recent_7)
    else:
        avg_daily = used / days_elapsed

    print(f"\n{'─' * 60}")
    print("PROJECTIONS")
    print(f"{'─' * 60}")

    # Current rate
    monthly_current = avg_daily * 30
    print(f"\nAvg daily burn (last 7d): {avg_daily:.0f} credits/day")
    print(f"Projected monthly (2 sports): {monthly_current:,.0f} / {total_budget:,}")
    headroom = total_budget - monthly_current
    print(f"Monthly headroom: {headroom:,.0f} credits")

    # Per-sport cost from recent data
    print(f"\n{'─' * 60}")
    print("PER-SPORT COST (last 7 days)")
    print(f"{'─' * 60}")
    sport_daily_cost = {}
    for r in per_sport_daily:
        endpoint = r["endpoint"]
        daily_cost = r["credits"] / 7
        sport_daily_cost[endpoint] = daily_cost
        print(f"{endpoint:<45} {daily_cost:>6.0f} credits/day")

    # Average per-sport cost (for estimating NHL)
    odds_endpoints = [k for k in sport_daily_cost if "/odds" in k]
    if odds_endpoints:
        avg_sport_cost = sum(sport_daily_cost[k] for k in odds_endpoints) / len(odds_endpoints)
    else:
        avg_sport_cost = avg_daily / 2  # rough split

    score_endpoints = [k for k in sport_daily_cost if "/scores" in k]
    if score_endpoints:
        avg_score_cost = sum(sport_daily_cost[k] for k in score_endpoints) / len(score_endpoints)
    else:
        avg_score_cost = 2  # 2 credits per score fetch

    nhl_daily_estimate = avg_sport_cost + avg_score_cost

    print(f"\n{'─' * 60}")
    print("NHL PROJECTION")
    print(f"{'─' * 60}")
    print(f"Estimated NHL daily cost: ~{nhl_daily_estimate:.0f} credits/day")
    print(f"  (odds polling: ~{avg_sport_cost:.0f} + score fetches: ~{avg_score_cost:.0f})")
    projected_with_nhl = monthly_current + (nhl_daily_estimate * 30)
    print(f"\nProjected monthly (3 sports): {projected_with_nhl:,.0f} / {total_budget:,}")
    if projected_with_nhl <= total_budget:
        print(f"✅ FITS within budget — {total_budget - projected_with_nhl:,.0f} credits to spare")
    else:
        overage = projected_with_nhl - total_budget
        print(f"⚠️  OVER BUDGET by {overage:,.0f} credits/month")
        # What interval would work?
        ratio = total_budget / projected_with_nhl
        needed_interval = 7 / ratio
        print(f"   To fit 3 sports, increase poll interval to ~{needed_interval:.0f} min")
        print(f"   Or upgrade to next tier (100K credits)")
