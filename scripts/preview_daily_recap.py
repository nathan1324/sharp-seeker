"""Preview the daily recap tweet text WITHOUT posting it.

READ-ONLY / NO NETWORK. Builds the exact text post_daily_recap would tweet,
using the deduped recap queries, so we can eyeball the corrected units before
firing a real (public) tweet. Does NOT call the X API and does NOT generate the
card image.

Usage (server):
    docker compose exec sharp-seeker python /app/scripts/preview_daily_recap.py
    # reproduce a past recap window (corrected):
    #   ... preview_daily_recap.py <since_iso> <asof_iso>
    # asof caps resolved_at so we see exactly what that day's recap should have read.
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone

from sharp_seeker.alerts.x_poster import XPoster, _month_start_iso
from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository

SINCE_ARG = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
ASOF_ARG = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else None


async def main() -> None:
    settings = Settings()  # type: ignore[call-arg]
    db = await init_db(settings.db_path)
    repo = Repository(db)

    since = SINCE_ARG or (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    results = await repo.get_free_play_results_resolved_since(since)
    mtd_results = await repo.get_free_play_results_resolved_since(_month_start_iso())

    # Cap on resolved_at to reproduce a recap as it stood at a past moment.
    if ASOF_ARG:
        results = [r for r in results if (dict(r).get("resolved_at") or "") <= ASOF_ARG]
        mtd_results = [r for r in mtd_results if (dict(r).get("resolved_at") or "") <= ASOF_ARG]
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
