"""Preview the daily recap tweet text WITHOUT posting it.

READ-ONLY / NO NETWORK. Builds the exact text post_daily_recap would tweet,
using the deduped recap queries, so we can eyeball the corrected units before
firing a real (public) tweet. Does NOT call the X API and does NOT generate the
card image.

Usage (server):
    docker compose exec sharp-seeker python /app/scripts/preview_daily_recap.py
"""

import asyncio
from datetime import datetime, timedelta, timezone

from sharp_seeker.alerts.x_poster import XPoster, _month_start_iso
from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository


async def main() -> None:
    settings = Settings()  # type: ignore[call-arg]
    db = await init_db(settings.db_path)
    repo = Repository(db)

    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    results = await repo.get_free_play_results_resolved_since(since)
    mtd_results = await repo.get_free_play_results_resolved_since(_month_start_iso())
    await db.close()

    # Build the text via the real formatter (no posting).
    poster = XPoster(settings, repo)
    text = poster._format_recap(results, mtd_results)

    print("=== DAILY RECAP PREVIEW (not posted) ===")
    print(f"window since: {since}")
    print(f"resolved free plays in window: {len(results)}   mtd: {len(mtd_results)}")
    print(f"char count: {len(text)}\n")
    print(text)
    print("\n=== END PREVIEW (card image would also be attached) ===")


if __name__ == "__main__":
    asyncio.run(main())
