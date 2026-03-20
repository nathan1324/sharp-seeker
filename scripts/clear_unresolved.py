"""Mark all unresolved signals as 'void' so the grader starts fresh.

Run after config changes to avoid grading signals from the old config.

Usage:
    docker compose exec sharp-seeker python /app/scripts/clear_unresolved.py
"""

import sqlite3
import time

DB = "/app/data/sharp_seeker.db"


def main():
    for attempt in range(10):
        try:
            conn = sqlite3.connect(DB, timeout=10)
            conn.row_factory = sqlite3.Row
            break
        except sqlite3.OperationalError:
            print(f"DB locked, retrying ({attempt + 1}/10)...")
            time.sleep(3)
    else:
        raise SystemExit("ERROR: Could not acquire DB lock.")

    # Count unresolved
    cur = conn.execute("SELECT COUNT(*) AS cnt FROM signal_results WHERE result IS NULL")
    count = cur.fetchone()["cnt"]
    print(f"Unresolved signals: {count}")

    if count == 0:
        print("Nothing to clear.")
        conn.close()
        return

    # Show a sample before clearing
    rows = conn.execute("""
        SELECT event_id, signal_type, market_key, outcome_name, signal_at
        FROM signal_results WHERE result IS NULL
        ORDER BY signal_at DESC LIMIT 10
    """).fetchall()
    print(f"\nSample (most recent {len(rows)}):")
    for r in rows:
        print(f"  {r['signal_at'][:16]}  {r['signal_type']:25s}  {r['market_key']:8s}  {r['outcome_name']}")

    # Delete unresolved rows
    conn.execute("DELETE FROM signal_results WHERE result IS NULL")
    conn.commit()
    print(f"\nDeleted {count} unresolved signal rows.")
    conn.close()


if __name__ == "__main__":
    main()
