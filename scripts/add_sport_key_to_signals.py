"""One-time migration: add sport_key column to signal_results and backfill."""

import sqlite3
import sys

DB_PATH = "/app/data/sharp_seeker.db"


def migrate(db_path: str = DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if column already exists
    cursor.execute("PRAGMA table_info(signal_results)")
    columns = {row[1] for row in cursor.fetchall()}
    if "sport_key" in columns:
        print("Column 'sport_key' already exists — skipping ALTER.")
    else:
        cursor.execute("ALTER TABLE signal_results ADD COLUMN sport_key TEXT")
        conn.commit()
        print("Added 'sport_key' column to signal_results.")

    # Backfill from odds_snapshots
    cursor.execute("""
        UPDATE signal_results
        SET sport_key = (
            SELECT os.sport_key
            FROM odds_snapshots os
            WHERE os.event_id = signal_results.event_id
            LIMIT 1
        )
        WHERE sport_key IS NULL
    """)
    backfilled = cursor.rowcount
    conn.commit()
    print(f"Backfilled {backfilled} rows with sport_key from odds_snapshots.")

    conn.close()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    migrate(path)
