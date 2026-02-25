"""Check NHL events currently available from the Odds API."""

import asyncio

from sharp_seeker.config import Settings
from sharp_seeker.api.odds_client import OddsClient
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository


async def main():
    s = Settings()
    db = await init_db(s.db_path)
    repo = Repository(db)
    c = OddsClient(s, repo)
    try:
        data = await c._fetch_odds("icehockey_nhl")
        for evt in data:
            print(f"{evt['commence_time']}  {evt['away_team']} vs {evt['home_team']}")
        print(f"\nTotal: {len(data)} events")
    finally:
        await c.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
