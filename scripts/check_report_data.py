"""Check what signal data exists for reports."""

import sqlite3
from datetime import datetime, timedelta, timezone

db = sqlite3.connect("/app/data/sharp_seeker.db")
db.row_factory = sqlite3.Row

since_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
since_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

print(f"=== Resolved signals by type (last 24h, signal_at >= {since_24h[:16]}) ===")
rows = db.execute(
    "SELECT signal_type, result, COUNT(*) as cnt FROM signal_results "
    "WHERE result IS NOT NULL AND signal_at >= ? GROUP BY signal_type, result",
    (since_24h,),
).fetchall()
for r in rows:
    print(f"  {r['signal_type']:30s} {r['result']:6s} {r['cnt']}")
if not rows:
    print("  (none)")

print(f"\n=== Resolved signals by type (last 7d, signal_at >= {since_7d[:16]}) ===")
rows = db.execute(
    "SELECT signal_type, result, COUNT(*) as cnt FROM signal_results "
    "WHERE result IS NOT NULL AND signal_at >= ? GROUP BY signal_type, result",
    (since_7d,),
).fetchall()
for r in rows:
    print(f"  {r['signal_type']:30s} {r['result']:6s} {r['cnt']}")
if not rows:
    print("  (none)")

print(f"\n=== Resolved signals by type (ALL TIME) ===")
rows = db.execute(
    "SELECT signal_type, result, COUNT(*) as cnt FROM signal_results "
    "WHERE result IS NOT NULL GROUP BY signal_type, result",
).fetchall()
for r in rows:
    print(f"  {r['signal_type']:30s} {r['result']:6s} {r['cnt']}")

print(f"\n=== Signals resolved today (resolved_at >= {since_24h[:16]}) ===")
rows = db.execute(
    "SELECT signal_type, COUNT(*) as cnt FROM signal_results "
    "WHERE result IS NOT NULL AND resolved_at >= ? GROUP BY signal_type",
    (since_24h,),
).fetchall()
for r in rows:
    print(f"  {r['signal_type']:30s} {r['cnt']}")
if not rows:
    print("  (none)")

db.close()
