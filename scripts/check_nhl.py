"""Check NHL events currently available from the Odds API."""

import asyncio

from sharp_seeker.config import Settings
from sharp_seeker.api.odds_client import OddsClient


async def main():
    s = Settings()
    async with OddsClient(s) as c:
        data = await c._fetch_odds("icehockey_nhl")
        for evt in data:
            print(f"{evt['commence_time']}  {evt['away_team']} vs {evt['home_team']}")
        print(f"\nTotal: {len(data)} events")


if __name__ == "__main__":
    asyncio.run(main())
