import sqlite3
import shutil
import sys

db = "/app/data/sharp_seeker.db"
today = "2026-02-20"
apply = "--apply" in sys.argv

conn = sqlite3.connect(db)
sql = (
    "SELECT COUNT(*) FROM signal_results "
    "WHERE signal_at < ? AND id NOT IN "
    "(SELECT MIN(id) FROM signal_results WHERE signal_at < ? "
    "GROUP BY event_id, signal_type, market_key)"
)
count = conn.execute(sql, (today, today)).fetchone()[0]
total = conn.execute("SELECT COUNT(*) FROM signal_results").fetchone()[0]
print("Duplicates to delete:", count)
print("Total rows:", total, " After:", total - count)

if not apply:
    print("Dry run. Re-run with --apply to delete.")
    conn.close()
    raise SystemExit(0)

shutil.copy2(db, db + ".bak")
conn.execute(
    "DELETE FROM signal_results "
    "WHERE signal_at < ? AND id NOT IN "
    "(SELECT MIN(id) FROM signal_results WHERE signal_at < ? "
    "GROUP BY event_id, signal_type, market_key)",
    (today, today),
)
conn.commit()
print("Done. Backed up DB and deleted", count, "rows.")
conn.close()
