"""Post a one-off FOMO recap tweet with free play stats.

Usage:
    docker compose exec sharp-seeker python /app/scripts/post_fomo_tweet.py

Set DRY_RUN=1 to preview without posting:
    docker compose exec -e DRY_RUN=1 sharp-seeker python /app/scripts/post_fomo_tweet.py
"""

import os
import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import tweepy
from dotenv import load_dotenv

load_dotenv()

DB = os.getenv("DB_PATH", "/app/data/sharp_seeker.db")
MST = ZoneInfo("America/Phoenix")
SINCE_STREAK = "2026-03-05"
SINCE_ALGO = "2026-03-01T00:00:00+00:00"
CTA = os.getenv("X_CTA_URL", "")
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"


def get_stats(conn):
    """Pull overall record, since-March-1 record, and current streak."""
    cur = conn.cursor()

    # Overall
    overall = cur.execute("""
        SELECT sr.result, COUNT(*) as cnt
        FROM sent_alerts sa
        JOIN signal_results sr
          ON sa.event_id = sr.event_id
         AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key
         AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1
          AND sr.result IN ('won', 'lost')
        GROUP BY sr.result
    """).fetchall()
    overall_w = sum(r[1] for r in overall if r[0] == "won")
    overall_l = sum(r[1] for r in overall if r[0] == "lost")

    # Since March 1
    since = cur.execute("""
        SELECT sr.result, COUNT(*) as cnt
        FROM sent_alerts sa
        JOIN signal_results sr
          ON sa.event_id = sr.event_id
         AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key
         AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1
          AND sa.sent_at >= ?
          AND sr.result IN ('won', 'lost')
        GROUP BY sr.result
    """, (SINCE_ALGO,)).fetchall()
    since_w = sum(r[1] for r in since if r[0] == "won")
    since_l = sum(r[1] for r in since if r[0] == "lost")

    # Current streak (from March 5 onward)
    streak_rows = cur.execute("""
        SELECT sr.result
        FROM sent_alerts sa
        JOIN signal_results sr
          ON sa.event_id = sr.event_id
         AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key
         AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1
          AND sa.sent_at >= ?
          AND sr.result IN ('won', 'lost')
        ORDER BY sa.sent_at ASC
    """, (SINCE_STREAK,)).fetchall()

    streak = 0
    if streak_rows:
        last = streak_rows[-1][0]
        for r in reversed(streak_rows):
            if r[0] == last:
                streak += 1
            else:
                break

    return {
        "overall_w": overall_w,
        "overall_l": overall_l,
        "since_w": since_w,
        "since_l": since_l,
        "streak": streak,
    }


def build_tweet(stats):
    """Build the FOMO tweet text."""
    streak = stats["streak"]
    checks = "\u2705" * streak

    overall_w = stats["overall_w"]
    overall_l = stats["overall_l"]
    overall_total = overall_w + overall_l
    overall_pct = round(overall_w / overall_total * 100) if overall_total else 0

    since_w = stats["since_w"]
    since_l = stats["since_l"]
    since_total = since_w + since_l
    since_pct = round(since_w / since_total * 100) if since_total else 0

    lines = [
        f"Our last {streak} free plays:",
        checks,
        "",
        f"{overall_w}-{overall_l} all time ({overall_pct}%)",
        f"{since_w}-{since_l} since our last algorithm update ({since_pct}%)",
        "",
        "Every pick posted publicly with the odds and book BEFORE tip-off.",
        "Every result auto-graded against final scores.",
        "",
        "You\u2019re either following this or you\u2019re guessing.",
    ]
    if CTA:
        lines.append(f"\n{CTA}")

    return "\n".join(lines)


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    stats = get_stats(conn)
    conn.close()

    tweet = build_tweet(stats)
    print("=== TWEET PREVIEW ===")
    print(tweet)
    print(f"\n({len(tweet)} chars)")

    if DRY_RUN:
        print("\n[DRY RUN — not posting]")
        return

    client = tweepy.Client(
        consumer_key=os.getenv("X_CONSUMER_KEY"),
        consumer_secret=os.getenv("X_CONSUMER_SECRET"),
        access_token=os.getenv("X_ACCESS_TOKEN"),
        access_token_secret=os.getenv("X_ACCESS_TOKEN_SECRET"),
    )
    resp = client.create_tweet(text=tweet)
    tweet_id = resp.data["id"]
    print(f"\nPosted! https://x.com/i/status/{tweet_id}")


if __name__ == "__main__":
    main()
