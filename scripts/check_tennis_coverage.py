"""Check tennis bookmaker and market coverage on The Odds API.

Usage (on server):
    docker compose exec sharp-seeker python /app/scripts/check_tennis_coverage.py

Costs: 0 credits for /sports endpoint, 1 credit per odds request.
"""

import asyncio

import httpx

from sharp_seeker.config import Settings


async def main():
    s = Settings()
    base = s.odds_api_base_url
    key = s.odds_api_key

    # 1. Find all in-season tennis sports (free endpoint, 0 credits)
    print("=" * 60)
    print("STEP 1: Fetching in-season tennis sports (0 credits)")
    print("=" * 60)

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(f"{base}/sports", params={"apiKey": key})
        resp.raise_for_status()
        sports = resp.json()

    tennis_sports = [s for s in sports if s.get("group", "").lower() == "tennis"]

    if not tennis_sports:
        print("\nNo in-season tennis sports found. Try again during a tournament.")
        return

    for t in tennis_sports:
        print(f"  {t['key']:45s}  {t.get('title', '')}")

    # 2. For each tennis sport, check bookmaker + market coverage
    # Uses 'us' region to check our target books, plus 'eu' for Pinnacle
    target_books = {"draftkings", "fanduel", "betmgm", "pinnacle"}
    markets = ["h2h", "spreads", "totals"]

    print(f"\nFound {len(tennis_sports)} tennis sport(s). Checking coverage...\n")
    print("=" * 60)
    print("STEP 2: Checking bookmaker & market coverage")
    print(f"  (costs up to {len(tennis_sports) * len(markets)} credits)")
    print("=" * 60)

    async with httpx.AsyncClient(timeout=30) as client:
        for sport in tennis_sports:
            sport_key = sport["key"]
            title = sport.get("title", sport_key)
            print(f"\n{'─' * 60}")
            print(f"  {title} ({sport_key})")
            print(f"{'─' * 60}")

            for market in markets:
                resp = await client.get(
                    f"{base}/sports/{sport_key}/odds",
                    params={
                        "apiKey": key,
                        "regions": "us,eu",
                        "markets": market,
                        "oddsFormat": "american",
                    },
                )

                remaining = resp.headers.get("x-requests-remaining", "?")

                if resp.status_code == 422:
                    print(f"    {market:10s} — not available for this sport")
                    continue

                resp.raise_for_status()
                events = resp.json()

                if not events:
                    print(f"    {market:10s} — no events with odds")
                    continue

                # Collect all bookmakers across events
                all_books = set()
                events_with_market = 0
                for event in events:
                    for bm in event.get("bookmakers", []):
                        all_books.add(bm["key"])
                    if event.get("bookmakers"):
                        events_with_market += 1

                found_targets = all_books & target_books
                missing_targets = target_books - all_books

                print(f"    {market:10s} — {events_with_market} events, {len(all_books)} bookmakers")
                print(f"      Our books:  {', '.join(sorted(found_targets)) or 'NONE'}")
                if missing_targets:
                    print(f"      Missing:    {', '.join(sorted(missing_targets))}")
                print(f"      All books:  {', '.join(sorted(all_books))}")

                print(f"      (credits remaining: {remaining})")

    print(f"\n{'=' * 60}")
    print("DONE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
