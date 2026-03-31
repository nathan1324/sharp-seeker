"""Quick check: PD NBA ML historical performance."""

import json
import sqlite3
import time
from collections import defaultdict

DB = "/app/data/sharp_seeker.db"


def connect():
    for attempt in range(10):
        try:
            conn = sqlite3.connect(DB, timeout=10)
            conn.row_factory = sqlite3.Row
            conn.execute("SELECT 1 FROM signal_results LIMIT 1")
            return conn
        except sqlite3.OperationalError:
            time.sleep(3)
    raise SystemExit("ERROR: Could not acquire DB lock.")


def compute_units(price, result, multiplier=1):
    if result == "push" or price is None:
        return 0.0
    if price < 0:
        risk = abs(price) / 100.0
    else:
        risk = 100.0 / price if price > 0 else 1.0
    if result == "won":
        return 1.0 * multiplier
    elif result == "lost":
        return -risk * multiplier
    return 0.0


conn = connect()
cur = conn.execute("""
    SELECT signal_at, result, details_json, signal_strength
    FROM signal_results
    WHERE result IS NOT NULL
      AND signal_type = 'pinnacle_divergence'
      AND sport_key = 'basketball_nba'
      AND market_key = 'h2h'
    ORDER BY signal_at
""")
rows = [dict(r) for r in cur.fetchall()]
conn.close()

if not rows:
    print("No PD NBA ML signals found.")
    exit()

print(f"PD NBA ML: {len(rows)} total graded signals")
print(f"Range: {rows[0]['signal_at'][:10]} to {rows[-1]['signal_at'][:10]}")

# Enrich
for r in rows:
    r["best_price"] = None
    details_raw = r.get("details_json")
    if details_raw:
        try:
            details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
            vb = details.get("value_books", [])
            if vb:
                r["best_price"] = vb[0].get("price")
        except (json.JSONDecodeError, TypeError):
            pass

w = sum(1 for r in rows if r["result"] == "won")
l = sum(1 for r in rows if r["result"] == "lost")
p = sum(1 for r in rows if r["result"] == "push")
u = sum(compute_units(r["best_price"], r["result"]) for r in rows)
print(f"Overall: {w}W-{l}L-{p}P ({w/(w+l):.0%}) [{u:+.1f}u]")

# By month
by_month = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
for r in rows:
    month = r["signal_at"][:7]
    by_month[month][r["result"]] += 1
    by_month[month]["units"] += compute_units(r["best_price"], r["result"])

print("\nBy month:")
for month in sorted(by_month.keys()):
    d = by_month[month]
    decided = d["won"] + d["lost"]
    wr = d["won"] / decided if decided > 0 else 0
    print(f"  {month}  {d['won']}W-{d['lost']}L-{d['push']}P  ({wr:.0%})  [{d['units']:+.1f}u]")

# By strength bucket
print("\nBy strength:")
buckets = [(0, 0.33, "<33%"), (0.33, 0.50, "33-49%"), (0.50, 0.67, "50-66%"), (0.67, 0.80, "67-79%"), (0.80, 1.01, "80%+")]
by_str = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
for r in rows:
    for lo, hi, label in buckets:
        if lo <= r["signal_strength"] < hi:
            by_str[label][r["result"]] += 1
            by_str[label]["units"] += compute_units(r["best_price"], r["result"])
            break
for _, _, label in buckets:
    if label in by_str:
        d = by_str[label]
        decided = d["won"] + d["lost"]
        wr = d["won"] / decided if decided > 0 else 0
        n = decided + d["push"]
        print(f"  {label:10s}  (n={n})  {d['won']}W-{d['lost']}L-{d['push']}P  ({wr:.0%})  [{d['units']:+.1f}u]")

# By value book
print("\nBy value book:")
by_book = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "units": 0.0})
for r in rows:
    book = "unknown"
    details_raw = r.get("details_json")
    if details_raw:
        try:
            details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
            vb = details.get("value_books", [])
            if vb:
                book = vb[0].get("bookmaker", "unknown")
        except (json.JSONDecodeError, TypeError):
            pass
    by_book[book][r["result"]] += 1
    by_book[book]["units"] += compute_units(r["best_price"], r["result"])

for book in sorted(by_book.keys(), key=lambda k: by_book[k]["units"]):
    d = by_book[book]
    decided = d["won"] + d["lost"]
    wr = d["won"] / decided if decided > 0 else 0
    n = decided + d["push"]
    print(f"  {book:20s}  (n={n})  {d['won']}W-{d['lost']}L-{d['push']}P  ({wr:.0%})  [{d['units']:+.1f}u]")

print("\nDone.")
