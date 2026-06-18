"""Re-post a past free play to X with the current formatter (incl. game time).

A free play already went out before the game date/time line was added, so it
read like it could be a live bet. This rebuilds the SAME play from the stored
sent_alerts row and re-posts it through the live XPoster formatter — which now
appends the commence_time (ET) — then drops the tweet link in Discord.

It does NOT insert a new sent_alerts row, so the daily recap is unaffected
(the play is already recorded). The re-post differs from the original tweet
(it has the new date line), so X won't reject it as a duplicate.

Usage (server):
    # dry run — print the tweet that WOULD be sent:
    docker compose exec sharp-seeker python /app/scripts/resend_free_play.py Giants
    # actually post it:
    docker compose exec sharp-seeker python /app/scripts/resend_free_play.py Giants --send

Args: [team_substring] [--send] [db_path=/app/data/sharp_seeker.db]
"""

import json
import sqlite3
import sys

from sharp_seeker.alerts.x_poster import XPoster
from sharp_seeker.config import Settings
from sharp_seeker.engine.base import Signal, SignalType

argv = [a for a in sys.argv[1:]]
SEND = "--send" in argv
argv = [a for a in argv if a != "--send"]
TEAM = argv[0] if argv else "Giants"
DB_PATH = argv[1] if len(argv) > 1 else "/app/data/sharp_seeker.db"


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    # Most recent free play whose matchup or outcome mentions the team.
    sql = """
        SELECT sa.id, sa.event_id, sa.alert_type, sa.market_key, sa.outcome_name,
          sa.sent_at, sa.details_json,
          (SELECT o.away_team || ' @ ' || o.home_team FROM odds_snapshots o
             WHERE o.event_id = sa.event_id LIMIT 1) AS matchup,
          (SELECT MAX(o.commence_time) FROM odds_snapshots o
             WHERE o.event_id = sa.event_id) AS commence_time,
          (SELECT o.sport_key FROM odds_snapshots o
             WHERE o.event_id = sa.event_id LIMIT 1) AS sport_key
        FROM sent_alerts sa
        WHERE sa.is_free_play = 1
          AND (sa.outcome_name LIKE ? OR sa.details_json LIKE ?)
        ORDER BY sa.sent_at DESC
    """
    like = "%" + TEAM + "%"
    rows = [dict(r) for r in db.execute(sql, (like, like))]
    db.close()

    if not rows:
        print("No free play found matching '" + TEAM + "'.")
        return

    if len(rows) > 1:
        print("Found " + str(len(rows)) + " matching free plays (using the most recent):")
        for r in rows:
            print("  " + str(r["sent_at"])[:16] + "  " + str(r["matchup"])
                  + "  " + r["outcome_name"] + " (" + r["market_key"] + ")")
        print("")

    r = rows[0]
    details = json.loads(r["details_json"]) if r["details_json"] else {}

    sig = Signal(
        signal_type=SignalType(r["alert_type"]),
        event_id=r["event_id"],
        sport_key=r["sport_key"] or "",
        home_team=details.get("home_team", ""),
        away_team=details.get("away_team", ""),
        market_key=r["market_key"],
        outcome_name=r["outcome_name"],
        strength=details.get("strength", 0.0) or 0.0,
        description="",
        commence_time=r["commence_time"] or "",
        details=details,
    )

    # repo is unused by _format_free_play / _post_tweet / _notify_discord.
    poster = XPoster(Settings(), repo=None)
    text = poster._format_free_play(sig)

    print("Original sent_at: " + str(r["sent_at"]))
    print("-" * 50)
    print(text)
    print("-" * 50)

    if not SEND:
        print("DRY RUN — re-run with --send to actually post.")
        return

    url = poster._post_tweet(text)
    print("Posted: " + str(url))
    if url:
        poster._notify_discord(url)
        print("Discord notified.")


if __name__ == "__main__":
    main()
