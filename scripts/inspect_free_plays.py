"""Inspect free plays and why they do/don't appear in the daily recap.

READ-ONLY. The daily X recap uses an INNER JOIN of sent_alerts (is_free_play=1)
to signal_results on (event_id, alert_type=signal_type, market_key,
outcome_name), filtered to signal_results.resolved_at >= now-24h. A free play
silently drops out of the recap if it is ungraded (resolved_at NULL), graded
outside the 24h window, or has NO matching signal_results row at all.

This lists every free play in a window and shows, per play: whether it matched
a signal_results row, its result, resolved_at, and whether it would survive the
recap's INNER JOIN + 24h resolved_at filter. That pinpoints the missing play.

Usage (server):
    docker compose exec sharp-seeker python /app/scripts/inspect_free_plays.py
Optional args: [hours_back] [db_path]   e.g. ... inspect_free_plays.py 48
"""

import sqlite3
import sys
from datetime import datetime, timedelta, timezone

HOURS_BACK = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 48
DB_PATH = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "/app/data/sharp_seeker.db"

# The recap's own window: resolved_at >= now - 24h
recap_since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
list_since = (datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)).isoformat()


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    # All free plays sent in the wider window, with their (possibly absent)
    # graded result via the SAME join keys the recap uses.
    sql = """
        SELECT sa.event_id, sa.alert_type, sa.market_key, sa.outcome_name,
               sa.sent_at, sr.result, sr.resolved_at, sr.sport_key,
               CASE WHEN sr.event_id IS NULL THEN 0 ELSE 1 END AS matched
        FROM sent_alerts sa
        LEFT JOIN signal_results sr
          ON sa.event_id = sr.event_id
         AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key
         AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1 AND sa.sent_at >= ?
        ORDER BY sa.sent_at ASC
    """
    rows = [dict(r) for r in db.execute(sql, (list_since,))]
    db.close()

    print(f"Free plays in last {HOURS_BACK}h - DB: {DB_PATH}")
    print(f"Recap window (resolved_at >=): {recap_since}\n")

    if not rows:
        print("No free plays in window.")
        return

    in_recap = 0
    wins = losses = pushes = 0
    print("  {:<22} {:<8} {:<7} {:<22} {:<8} {:<26} {}".format(
        "event_id", "type", "market", "outcome", "result", "resolved_at", "recap?"))
    print("  " + "-" * 110)
    for r in rows:
        matched = r["matched"] == 1
        result = r["result"]
        resolved_at = r["resolved_at"]
        # Replicate the recap's INNER JOIN + resolved_at >= recap_since
        survives = bool(matched and resolved_at is not None and resolved_at >= recap_since)
        if survives:
            in_recap += 1
            if result == "won":
                wins += 1
            elif result == "lost":
                losses += 1
            elif result == "push":
                pushes += 1

        if not matched:
            why = "NO signal_results match"
        elif resolved_at is None:
            why = "UNGRADED (resolved_at NULL)"
        elif resolved_at < recap_since:
            why = "graded before window"
        else:
            why = "IN RECAP"

        ev = (r["event_id"] or "")[:22]
        oc = (r["outcome_name"] or "")[:22]
        print("  {:<22} {:<8} {:<7} {:<22} {:<8} {:<26} {}".format(
            ev, (r["alert_type"] or "")[:8], (r["market_key"] or "")[:7],
            oc, str(result), str(resolved_at)[:26], why))

    print(f"\n  Sent in window: {len(rows)}   |   Survive recap join+window: {in_recap}")
    print(f"  Recap record would read: {wins}-{losses}" + (f" ({pushes} push)" if pushes else ""))
    dropped = len(rows) - in_recap
    if dropped:
        print(f"\n  >>> {dropped} free play(s) sent but NOT in the recap — see 'recap?' column above.")


if __name__ == "__main__":
    main()
