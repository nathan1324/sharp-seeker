"""Post a one-off correction tweet for the double-counted 6/3 recap.

Posts ONLY when run with the literal argument POST; otherwise it is a dry run
that prints the text and character count. Run a dry run first to eyeball the
rendered emojis/length before posting.

Usage (server):
    # dry run (prints, does not post):
    docker compose exec sharp-seeker python /app/scripts/post_correction_tweet.py
    # actually post:
    docker compose exec sharp-seeker python /app/scripts/post_correction_tweet.py POST
"""

import sys

from sharp_seeker.alerts.x_poster import XPoster
from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository

import asyncio

TEXT = (
    "\U0001F4CA Correction: our 6/3 recap double-counted a play "
    "(NYK/SAS U218.5) due to a bug, showing 7-1. The accurate result was "
    "4-1 (+3.8u). Fixed & verified going forward.\n\n"
    "✅ NYK/SAS U218.5\n"
    "✅ MIA/WSH U8.5\n"
    "❌ CLE/NYY U7.5\n"
    "✅ LAD/ARI U9\n"
    "✅ SF/MIL U8.5"
)


async def main() -> None:
    do_post = len(sys.argv) > 1 and sys.argv[1] == "POST"
    settings = Settings()  # type: ignore[call-arg]
    db = await init_db(settings.db_path)
    repo = Repository(db)
    poster = XPoster(settings, repo)

    # Twitter weights most emoji as 2 chars; add the emoji count as a cushion.
    emoji_count = sum(TEXT.count(e) for e in ("\U0001F4CA", "✅", "❌"))
    weighted = len(TEXT) + emoji_count
    print(f"enabled={poster._enabled}  raw_len={len(TEXT)}  ~weighted={weighted} (limit 280)\n")
    print(TEXT)
    print()

    if weighted > 280:
        print("ABORT: text exceeds 280 weighted chars — trim before posting.")
        await db.close()
        return

    if not do_post:
        print(">>> DRY RUN — pass POST to actually tweet.")
        await db.close()
        return

    if not poster._enabled:
        print("ABORT: X poster not enabled (missing credentials).")
        await db.close()
        return

    url = poster._post_tweet(TEXT)
    print(f">>> POSTED. url={url}")
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
