"""Manually trigger the daily recap tweet with card image attached."""

import asyncio

from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository
from sharp_seeker.analysis.card_generator import CardGenerator
from sharp_seeker.alerts.x_poster import XPoster


async def main():
    settings = Settings()
    db = await init_db(settings.db_path)
    repo = Repository(db)
    card_gen = CardGenerator(settings, repo)
    poster = XPoster(settings, repo, card_gen=card_gen)
    await poster.post_daily_recap()
    await db.close()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
