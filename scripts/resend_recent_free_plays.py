"""Re-post recent free plays to X with the current formatter (incl. game time).

Batch sibling of resend_free_play.py: instead of one play, it rebuilds EVERY
free play sent in the last N minutes (default 60) and re-posts each through the
live XPoster formatter — which now appends the commence_time (ET) — then drops
each tweet link in Discord. Use this after deleting auto-posted tweets that went
out before the date/time line existed.

It does NOT insert new sent_alerts rows, so the daily recap is unaffected. Each
re-post differs from its original tweet (it has the new date line), so X won't
reject it as a duplicate.

Safety: a game that has already started is skipped (re-posting it would recreate
the live-bet confusion). Override with --force only if you truly mean to.

Usage (server):
    # list what WOULD be resent (no posting):
    docker compose exec sharp-seeker python /app/scripts/resend_recent_free_plays.py
    # actually re-post them:
    docker compose exec sharp-seeker python /app/scripts/resend_recent_free_plays.py --send

Args: [--minutes N=60] [--send] [--force] [db_path=/app/data/sharp_seeker.db]
"""

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

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
MINUTES = int(_take("--minutes", "60"))
DB_PATH = argv[0] if argv else "/app/data/sharp_seeker.db"


def _has_started(commence_time, now):
    if not commence_time:
        return False
    try:
        ct = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return False
    if ct.tzinfo is None:
        ct = ct.replace(tzinfo=timezone.utc)
    return now >= ct


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc)
    since = (now - timedelta(minutes=MINUTES)).isoformat()

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
        WHERE sa.is_free_play = 1 AND sa.sent_at >= ?
        ORDER BY sa.sent_at ASC
    """
    rows = [dict(r) for r in db.execute(sql, (since,))]
    db.close()

    print("Free plays sent in the last " + str(MINUTES) + " min (since "
          + since[:16] + "): " + str(len(rows)))
    if not rows:
        return

    poster = XPoster(Settings(), repo=None)
    sent = skipped = 0

    for r in rows:
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
        text = poster._format_free_play(sig)
        started = _has_started(r["commence_time"], now)

        print("")
        print("=" * 50)
        print("sent " + str(r["sent_at"])[:16] + "  event " + r["event_id"]
              + ("   [STARTED]" if started else "   [pregame]"))
        print("-" * 50)
        print(text)

        if started and not FORCE:
            print("-> SKIP (game already started; use --force to override)")
            skipped += 1
            continue
        if not SEND:
            continue

        url = poster._post_tweet(text)
        print("-> posted: " + str(url))
        if url:
            poster._notify_discord(url)
        sent += 1

    print("")
    print("=" * 50)
    if SEND:
        print("Done. Posted " + str(sent) + ", skipped " + str(skipped)
              + " (started).")
    else:
        eligible = sum(1 for r in rows if not _has_started(r["commence_time"], now))
        print("DRY RUN — " + str(eligible) + " pregame play(s) would post, "
              + str(len(rows) - eligible) + " skipped (started). Re-run with --send.")


if __name__ == "__main__":
    main()
