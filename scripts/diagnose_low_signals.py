"""Diagnose why signal volume is low today."""

import sqlite3
from datetime import datetime, timedelta, timezone

DB = "/app/data/sharp_seeker.db"
MST = timezone(timedelta(hours=-7))


def run():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    now_utc = datetime.now(timezone.utc)
    today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    yesterday_start = (now_utc - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

    print(f"=== Signal Diagnostics — {now_utc.astimezone(MST).strftime('%Y-%m-%d %I:%M %p')} MST ===\n")

    # 1. Polls today (distinct fetched_at timestamps)
    cur.execute(
        "SELECT COUNT(DISTINCT fetched_at) FROM odds_snapshots WHERE fetched_at >= ?",
        (today_start,),
    )
    polls_today = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(DISTINCT fetched_at) FROM odds_snapshots WHERE fetched_at >= ? AND fetched_at < ?",
        (yesterday_start, today_start),
    )
    polls_yesterday = cur.fetchone()[0]
    print(f"Polls today:     {polls_today}  (yesterday: {polls_yesterday})")

    # 2. Last poll timestamp
    cur.execute("SELECT MAX(fetched_at) FROM odds_snapshots")
    last_poll = cur.fetchone()[0]
    if last_poll:
        last_dt = datetime.fromisoformat(last_poll).astimezone(MST)
        age = now_utc - datetime.fromisoformat(last_poll).replace(tzinfo=timezone.utc)
        print(f"Last poll:       {last_dt.strftime('%I:%M %p MST')}  ({int(age.total_seconds() // 60)} min ago)")
    else:
        print("Last poll:       NONE")

    # 3. Signals today vs yesterday
    cur.execute(
        "SELECT COUNT(*) FROM sent_alerts WHERE sent_at >= ?",
        (today_start,),
    )
    alerts_today = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM sent_alerts WHERE sent_at >= ? AND sent_at < ?",
        (yesterday_start, today_start),
    )
    alerts_yesterday = cur.fetchone()[0]
    print(f"Alerts today:    {alerts_today}  (yesterday: {alerts_yesterday})")

    # 4. Alerts by type today
    cur.execute(
        "SELECT alert_type, COUNT(*) as cnt FROM sent_alerts WHERE sent_at >= ? GROUP BY alert_type ORDER BY cnt DESC",
        (today_start,),
    )
    rows = cur.fetchall()
    if rows:
        print("\n  By type:")
        for r in rows:
            print(f"    {r['alert_type']:30s} {r['cnt']}")
    else:
        print("\n  No alerts today.")

    # 5. signal_results today (in case signals are being recorded but not alerted)
    cur.execute(
        "SELECT COUNT(*) FROM signal_results WHERE signal_at >= ?",
        (today_start,),
    )
    results_today = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM signal_results WHERE signal_at >= ? AND signal_at < ?",
        (yesterday_start, today_start),
    )
    results_yesterday = cur.fetchone()[0]
    print(f"\nSignal results today: {results_today}  (yesterday: {results_yesterday})")

    # 6. Games available today (distinct events in snapshots from today)
    cur.execute(
        "SELECT COUNT(DISTINCT event_id) FROM odds_snapshots WHERE fetched_at >= ?",
        (today_start,),
    )
    events_today = cur.fetchone()[0]
    print(f"Events polled today:  {events_today}")

    # 7. Sports polled today
    cur.execute(
        "SELECT sport_key, COUNT(DISTINCT event_id) as games FROM odds_snapshots WHERE fetched_at >= ? GROUP BY sport_key",
        (today_start,),
    )
    rows = cur.fetchall()
    if rows:
        print("\n  By sport:")
        for r in rows:
            print(f"    {r['sport_key']:30s} {r['games']} games")

    # 8. API credits remaining
    cur.execute("SELECT credits_remaining FROM api_usage ORDER BY timestamp DESC LIMIT 1")
    row = cur.fetchone()
    if row:
        print(f"\nAPI credits remaining: {row['credits_remaining']}")

    # 9. Last 5 alerts for context
    cur.execute(
        "SELECT alert_type, event_id, market_key, outcome_name, sent_at FROM sent_alerts ORDER BY sent_at DESC LIMIT 5"
    )
    rows = cur.fetchall()
    if rows:
        print("\nLast 5 alerts:")
        for r in rows:
            dt = datetime.fromisoformat(r["sent_at"]).astimezone(MST)
            print(f"  {dt.strftime('%m/%d %I:%M %p')}  {r['alert_type']:25s} {r['outcome_name']} ({r['market_key']})")

    conn.close()


if __name__ == "__main__":
    run()
