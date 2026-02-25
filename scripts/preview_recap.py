"""Preview the daily free play recap tweet without posting it."""

import asyncio
from datetime import datetime, timedelta, timezone

from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository


async def main():
    s = Settings()
    db = await init_db(s.db_path)
    repo = Repository(db)

    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    results = await repo.get_free_play_results_since(since)

    if not results:
        print("No free plays in the last 24 hours — recap would be skipped.")
    else:
        print(f"Found {len(results)} free play(s):\n")
        for row in results:
            d = dict(row)
            print(f"  event_id={d['event_id']}  outcome={d['outcome_name']}  "
                  f"market={d['market_key']}  result={d['result']}")

        # Format the tweet using XPoster._format_recap
        from sharp_seeker.alerts.x_poster import XPoster
        poster = XPoster(s, repo)
        poster._cta_url = s.x_cta_url
        text = poster._format_recap(results)
        print("\n--- Tweet preview ---\n")
        print(text)
        print(f"\n--- {len(text)} characters ---")

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
