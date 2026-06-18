"""Re-post a past free play to X with the current formatter (incl. game time).

A free play already went out before the game date/time line was added, so it
read like it could be a live bet. This rebuilds the SAME play from the stored
sent_alerts row and re-posts it through the live XPoster formatter — which now
appends the commence_time (ET) — then drops the tweet link in Discord.

It does NOT insert a new sent_alerts row, so the daily recap is unaffected
(the play is already recorded). The re-post differs from the original tweet
(it has the new date line), so X won't reject it as a duplicate.

Safety: re-posting a game that has ALREADY STARTED would recreate the very
live-bet confusion we're fixing, so --send is blocked once a game is underway
(override with --force only if you truly mean to).

Usage (server):
    # list matches with game time + status (no posting):
    docker compose exec sharp-seeker python /app/scripts/resend_free_play.py Giants
    # post a specific one (index from the list; default 0 = most recent):
    docker compose exec sharp-seeker python /app/scripts/resend_free_play.py Giants --index 1 --send

Args: [team_substring] [--index N] [--event ID] [--send] [--force] [db_path]
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone

from sharp_seeker.alerts.x_poster import XPoster, _format_game_time
from sharp_seeker.config import Settings
from sharp_seeker.engine.base import Signal, SignalType


def _take(flag, default=None):
    """Pop `--flag value` out of argv; return its value or default."""
    if flag in argv:
        i = argv.index(flag)
        val = argv[i + 1] if i + 1 < len(argv) else default
        del argv[i:i + 2]
        return val
    return default


argv = list(sys.argv[1:])
SEND = "--send" in argv
FORCE = "--force" in argv
argv = [a for a in argv if a not in ("--send", "--force")]
INDEX = int(_take("--index", "0"))
EVENT = _take("--event")
TEAM = argv[0] if argv else "Giants"
DB_PATH = argv[1] if len(argv) > 1 else "/app/data/sharp_seeker.db"


def _status(commence_time, now):
    """PREGAME/STARTED tag + countdown from commence_time."""
    if not commence_time:
        return "no game time"
    try:
        ct = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return "bad game time"
    if ct.tzinfo is None:
        ct = ct.replace(tzinfo=timezone.utc)
    delta = ct - now
    mins = int(abs(delta).total_seconds()) // 60
    hm = str(mins // 60) + "h " + str(mins % 60) + "m"
    return "PREGAME (starts in " + hm + ")" if delta.total_seconds() > 0 \
        else "STARTED (" + hm + " ago)"


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc)

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

    print("Matching free plays for '" + TEAM + "' (newest first):")
    for i, r in enumerate(rows):
        gt = _format_game_time(r["commence_time"] or "")
        print("  [" + str(i) + "] sent " + str(r["sent_at"])[:16]
              + "  " + str(r["matchup"]) + "  " + r["outcome_name"]
              + " (" + r["market_key"] + ")")
        print("        game: " + (gt or "?") + "   " + _status(r["commence_time"], now))
    print("")

    if EVENT:
        match = [r for r in rows if r["event_id"] == EVENT]
        if not match:
            print("No match with event_id " + EVENT)
            return
        r = match[0]
    else:
        if INDEX < 0 or INDEX >= len(rows):
            print("--index " + str(INDEX) + " out of range (0.." + str(len(rows) - 1) + ")")
            return
        r = rows[INDEX]

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

    print("Selected: event " + r["event_id"] + "  (sent " + str(r["sent_at"]) + ")")
    print("-" * 50)
    print(text)
    print("-" * 50)

    started = False
    if r["commence_time"]:
        try:
            ct = datetime.fromisoformat(r["commence_time"].replace("Z", "+00:00"))
            if ct.tzinfo is None:
                ct = ct.replace(tzinfo=timezone.utc)
            started = now >= ct
        except (ValueError, TypeError):
            pass

    if not SEND:
        print("DRY RUN — re-run with --send to actually post.")
        return

    if started and not FORCE:
        print("BLOCKED: this game has already started — re-posting would look "
              "like a live bet. Add --force only if you really mean to.")
        return

    url = poster._post_tweet(text)
    print("Posted: " + str(url))
    if url:
        poster._notify_discord(url)
        print("Discord notified.")


if __name__ == "__main__":
    main()
