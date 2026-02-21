"""Reset API credit tracking after a plan upgrade.

Inserts a fresh row so the budget tracker sees the new credit pool
and resumes polling. The next real API call will overwrite this with
the actual value from the response headers.
"""

import sqlite3
import sys
from datetime import datetime, timezone

db_path = sys.argv[1] if len(sys.argv) > 1 else "/app/data/sharp_seeker.db"
new_credits = int(sys.argv[2]) if len(sys.argv) > 2 else 20000

db = sqlite3.connect(db_path)
now = datetime.now(timezone.utc).isoformat()
db.execute(
    "INSERT INTO api_usage (timestamp, endpoint, credits_used, credits_remaining) VALUES (?, ?, ?, ?)",
    (now, "plan_upgrade_reset", 0, new_credits),
)
db.commit()
print(f"Reset credits_remaining to {new_credits}. Polling will resume on next cycle.")
