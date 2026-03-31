"""Check what's in details_json for signals with unknown value books."""

import json
import sqlite3
import time

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


conn = connect()
cur = conn.execute("""
    SELECT signal_at, signal_type, sport_key, market_key, outcome_name, details_json
    FROM signal_results
    WHERE result IS NOT NULL
    ORDER BY signal_at
    LIMIT 500
""")
rows = [dict(r) for r in cur.fetchall()]
conn.close()

unknown_count = 0
empty_vb = 0
no_details = 0
missing_key = 0
has_book = 0

for r in rows:
    raw = r.get("details_json")
    if not raw:
        no_details += 1
        continue
    try:
        details = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        no_details += 1
        continue

    vb = details.get("value_books", [])
    if not vb:
        empty_vb += 1
        continue

    first = vb[0]
    if "bookmaker" in first:
        has_book += 1
    elif "book" in first:
        has_book += 1
    else:
        missing_key += 1
        unknown_count += 1
        # Print first few to see structure
        if unknown_count <= 5:
            print(f"  {r['signal_at'][:16]}  {r['signal_type']:25s} {r['market_key']:8s}")
            print(f"    value_books[0] keys: {list(first.keys())}")
            print(f"    value_books[0]: {first}")
            print()

print(f"Checked first {len(rows)} signals:")
print(f"  Has 'bookmaker' key: {has_book}")
print(f"  Missing book key:    {missing_key}")
print(f"  Empty value_books:   {empty_vb}")
print(f"  No details_json:     {no_details}")

# Also check what the earliest signals look like
print("\nEarliest 3 signals with value_books:")
count = 0
for r in rows:
    raw = r.get("details_json")
    if not raw:
        continue
    details = json.loads(raw) if isinstance(raw, str) else raw
    vb = details.get("value_books", [])
    if vb:
        print(f"  {r['signal_at'][:16]}  {r['signal_type']}")
        print(f"    keys: {list(vb[0].keys())}")
        count += 1
        if count >= 3:
            break

print("\nDone.")
