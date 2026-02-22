"""One-time migration: add deep_link column to odds_snapshots."""

import sqlite3
import sys

DB_PATH = "/app/data/sharp_seeker.db"


def migrate(db_path: str = DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if column already exists
    cursor.execute("PRAGMA table_info(odds_snapshots)")
    columns = {row[1] for row in cursor.fetchall()}
    if "deep_link" in columns:
        print("Column 'deep_link' already exists — nothing to do.")
        conn.close()
        return

    cursor.execute("ALTER TABLE odds_snapshots ADD COLUMN deep_link TEXT")
    conn.commit()
    print("Added 'deep_link' column to odds_snapshots.")
    conn.close()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    migrate(path)
