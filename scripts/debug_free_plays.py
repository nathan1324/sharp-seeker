"""Debug: show recent free plays and their sent_at times."""

import json
import sqlite3
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

DB = "/app/data/sharp_seeker.db"
MST = ZoneInfo("America/Phoenix")


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    now = datetime.now(timezone.utc)
    since_48h = (now - timedelta(hours=48)).isoformat()
    since_24h = (now - timedelta(hours=24)).isoformat()

    print("=== Current time ===")
    print("  UTC:", now.isoformat())
    print("  MST:", now.astimezone(MST).isoformat())
    print("  24h cutoff:", since_24h)
    print()

    rows = conn.execute("""
        SELECT sa.event_id, sa.market_key, sa.outcome_name, sa.sent_at,
               sa.alert_type, sr.result
        FROM sent_alerts sa
        LEFT JOIN signal_results sr
          ON sa.event_id = sr.event_id
         AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key
         AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1
          AND sa.sent_at >= ?
        ORDER BY sa.sent_at DESC
    """, (since_48h,)).fetchall()

    print("=== Free plays in last 48h ===")
    for row in rows:
        r = dict(row)
        sent = r["sent_at"]
        in_24h = sent >= since_24h
        marker = "  IN 24h" if in_24h else "  OUTSIDE 24h <---"
        sent_mst = ""
        try:
            dt = datetime.fromisoformat(sent)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            sent_mst = dt.astimezone(MST).strftime("%b %d %I:%M %p MST")
        except Exception:
            pass

        print("  {outcome} ({market}) | sent: {sent} ({mst}) | result: {result}{marker}".format(
            outcome=r["outcome_name"],
            market=r["market_key"],
            sent=sent,
            mst=sent_mst,
            result=r["result"] or "pending",
            marker=marker,
        ))

    if not rows:
        print("  (none found)")

    conn.close()


if __name__ == "__main__":
    main()
