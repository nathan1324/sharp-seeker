"""One-time migration: add is_live column to signal_results and backfill."""

import sqlite3
import sys

DB_PATH = "/app/data/sharp_seeker.db"


def migrate(db_path: str = DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if column already exists
    cursor.execute("PRAGMA table_info(signal_results)")
    columns = {row[1] for row in cursor.fetchall()}
    if "is_live" in columns:
        print("Column 'is_live' already exists — skipping ALTER.")
    else:
        cursor.execute("ALTER TABLE signal_results ADD COLUMN is_live INTEGER")
        conn.commit()
        print("Added 'is_live' column to signal_results.")

    # Backfill: compare signal_at to commence_time from odds_snapshots
    # is_live = 1 if signal_at >= commence_time, 0 if signal_at < commence_time
    cursor.execute("""
        UPDATE signal_results
        SET is_live = (
            SELECT CASE
                WHEN signal_results.signal_at >= os.commence_time THEN 1
                ELSE 0
            END
            FROM odds_snapshots os
            WHERE os.event_id = signal_results.event_id
            LIMIT 1
        )
        WHERE is_live IS NULL
    """)
    backfilled = cursor.rowcount
    conn.commit()
    print(f"Backfilled {backfilled} rows with is_live from odds_snapshots.")

    conn.close()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    migrate(path)
