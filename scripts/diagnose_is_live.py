"""Diagnose is_live classification for signal_results."""

import sqlite3
import sys

DB_PATH = "/app/data/sharp_seeker.db"


def diagnose(db_path: str = DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Show format samples for signal_at and commence_time
    print("=== Format Samples ===")
    cursor.execute("""
        SELECT sr.signal_at, os.commence_time
        FROM signal_results sr
        JOIN odds_snapshots os ON os.event_id = sr.event_id
        GROUP BY sr.id
        ORDER BY sr.id DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"  signal_at={row['signal_at']}  commence_time={row['commence_time']}")

    # 2. Show is_live distribution for NCAAM
    print("\n=== NCAAM is_live distribution ===")
    cursor.execute("""
        SELECT is_live, result, COUNT(*) as cnt
        FROM signal_results
        WHERE sport_key LIKE '%ncaa%' AND result IS NOT NULL
        GROUP BY is_live, result
        ORDER BY is_live, result
    """)
    for row in cursor.fetchall():
        label = {0: "pregame", 1: "live", None: "NULL"}[row["is_live"]]
        print(f"  {label}: {row['result']} = {row['cnt']}")

    # 3. Spot-check: show individual NCAAM signals with timing context
    print("\n=== NCAAM Signal Timing Spot-Check (last 20) ===")
    cursor.execute("""
        SELECT sr.id, sr.signal_at, sr.is_live, sr.result,
               os.commence_time,
               sr.event_id
        FROM signal_results sr
        LEFT JOIN odds_snapshots os ON os.event_id = sr.event_id
        WHERE sr.sport_key LIKE '%ncaa%' AND sr.result IS NOT NULL
        GROUP BY sr.id
        ORDER BY sr.id DESC
        LIMIT 20
    """)
    for row in cursor.fetchall():
        ct = row["commence_time"] or "N/A"
        sa = row["signal_at"]
        is_live_db = row["is_live"]
        # Recompute with proper datetime parsing
        actual = "?"
        if ct != "N/A" and sa:
            from datetime import datetime, timezone
            try:
                sa_dt = datetime.fromisoformat(sa)
                ct_dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                # Ensure both are tz-aware for comparison
                if sa_dt.tzinfo is None:
                    sa_dt = sa_dt.replace(tzinfo=timezone.utc)
                actual = "live" if sa_dt >= ct_dt else "pregame"
            except Exception as e:
                actual = f"err: {e}"
        db_label = {0: "pregame", 1: "live", None: "NULL"}.get(is_live_db, "?")
        match = "OK" if (actual == db_label) else "MISMATCH"
        print(
            f"  [{match}] id={row['id']} db={db_label} actual={actual} "
            f"result={row['result']}  signal_at={sa}  commence={ct}"
        )

    conn.close()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    diagnose(path)
