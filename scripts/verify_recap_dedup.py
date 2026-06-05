"""Verify the recap fan-out fix on live data by calling the REAL repo methods.

READ-ONLY. Exercises the deployed get_free_play_results_resolved_since and
get_free_play_results_since over a wide window and asserts each free play
appears at most once, even when its signal fired across many poll cycles. Also
shows the raw signal_results fan-out per play so you can see the dedup working
(raw count > 1, recap count == 1).

Usage (server):
    docker compose exec sharp-seeker python /app/scripts/verify_recap_dedup.py
Optional arg: [days_back]  (default 30)
"""

import asyncio
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository

DAYS_BACK = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 30


async def main() -> None:
    settings = Settings()  # type: ignore[call-arg]
    db = await init_db(settings.db_path)
    repo = Repository(db)
    since = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).isoformat()

    resolved = await repo.get_free_play_results_resolved_since(since)
    sent_based = await repo.get_free_play_results_since(since)

    # Raw fan-out per play, straight from signal_results (what the OLD join did).
    raw_sql = """
        SELECT sr.event_id, sr.market_key, sr.outcome_name, COUNT(*) AS n
        FROM sent_alerts sa
        JOIN signal_results sr
          ON sa.event_id = sr.event_id AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1 AND sa.sent_at >= ?
        GROUP BY sr.event_id, sr.market_key, sr.outcome_name
    """
    cur = await db.execute(raw_sql, (since,))
    raw = {(r["event_id"], r["market_key"], r["outcome_name"]): r["n"]
           for r in await cur.fetchall()}
    await db.close()

    def key(row):
        d = dict(row)
        return (d["event_id"], d["market_key"], d["outcome_name"])

    print(f"verify_recap_dedup (last {DAYS_BACK}d) - DB: {settings.db_path}\n")

    ok = True
    for label, rows in (("resolved_since (daily/card)", resolved),
                        ("since (weekly/YTD/month)", sent_based)):
        counts = Counter(key(r) for r in rows)
        dupes = {k: c for k, c in counts.items() if c > 1}
        status = "OK" if not dupes else "FAIL"
        if dupes:
            ok = False
        print(f"  [{status}] {label}: {len(rows)} rows, {len(counts)} distinct plays")
        if dupes:
            for k, c in dupes.items():
                print(f"      DUPLICATE x{c}: {k}")

    # Show the dedup doing real work: plays that had multiple signal_results rows
    fanned = {k: n for k, n in raw.items() if n > 1}
    print(f"\n  Plays whose signal fired across multiple cycles (raw fan-out): {len(fanned)}")
    for k, n in sorted(fanned.items(), key=lambda kv: -kv[1])[:10]:
        in_resolved = sum(1 for r in resolved if key(r) == k)
        in_sent = sum(1 for r in sent_based if key(r) == k)
        print(f"      raw={n:>2}  recap(resolved)={in_resolved}  recap(since)={in_sent}  {k}")

    print("\n  RESULT:", "PASS - no play counted more than once" if ok
          else "FAIL - duplicates remain")


if __name__ == "__main__":
    asyncio.run(main())
