"""Quick test: verify Odds API key and check which bookmakers return data."""

import os
import requests

key = os.environ.get("ODDS_API_KEY", "")
if not key:
    print("ERROR: ODDS_API_KEY not set")
    exit(1)

url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"
r = requests.get(url, params={
    "apiKey": key,
    "regions": "us,eu",
    "bookmakers": "draftkings,fanduel,pinnacle,williamhill_us,betrivers",
    "markets": "spreads",
    "oddsFormat": "american",
})

print("Status: {}".format(r.status_code))
print("Credits remaining: {}".format(r.headers.get("x-requests-remaining")))
print("Credits used: {}".format(r.headers.get("x-requests-used")))

if r.status_code != 200:
    print("Error: {}".format(r.text))
    exit(1)

data = r.json()
if isinstance(data, list) and data:
    books = set()
    for evt in data:
        for bm in evt.get("bookmakers", []):
            books.add(bm["key"])
    print("Events: {}".format(len(data)))
    print("Books found: {}".format(sorted(books)))
else:
    print("No events returned")
