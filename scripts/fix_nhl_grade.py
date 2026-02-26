"""Fix the mis-graded NHL Pinnacle Divergence signal from 2026-02-25.

The Buffalo Sabres +1.5 (FanDuel) spread signal was graded against
Pinnacle's -1.5 line, resulting in "lost".  Buffalo won outright, so the
+1.5 bet actually won.  This script corrects the result to "won".
"""

import asyncio
from datetime import datetime, timezone

import aiosqlite


EVENT_ID = "9d0b725f6e813c43975b6ac5532309a6"
SIGNAL_TYPE = "pinnacle_divergence"
MARKET_KEY = "spreads"
OUTCOME_NAME = "Buffalo Sabres"
SIGNAL_AT = "2026-02-25T13:11:49.463903+00:00"


async def main() -> None:
    db = await aiosqlite.connect("/app/data/sharp_seeker.db")
    db.row_factory = aiosqlite.Row

    # Verify current state
    cursor = await db.execute(
        """SELECT result, resolved_at FROM signal_results
           WHERE event_id = ? AND signal_type = ? AND market_key = ?
             AND outcome_name = ? AND signal_at = ?""",
        (EVENT_ID, SIGNAL_TYPE, MARKET_KEY, OUTCOME_NAME, SIGNAL_AT),
    )
    row = await cursor.fetchone()

    if not row:
        print("Signal not found — nothing to fix.")
        await db.close()
        return

    row = dict(row)
    print(f"Current result: {row['result']}  resolved_at: {row['resolved_at']}")

    if row["result"] == "won":
        print("Already correct — no update needed.")
        await db.close()
        return

    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """UPDATE signal_results SET result = 'won', resolved_at = ?
           WHERE event_id = ? AND signal_type = ? AND market_key = ?
             AND outcome_name = ? AND signal_at = ?""",
        (now, EVENT_ID, SIGNAL_TYPE, MARKET_KEY, OUTCOME_NAME, SIGNAL_AT),
    )
    await db.commit()
    print(f"Updated to 'won' at {now}")

    await db.close()


asyncio.run(main())
