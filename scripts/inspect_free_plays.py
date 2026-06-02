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

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

HOURS_BACK = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 48
DB_PATH = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "/app/data/sharp_seeker.db"

# The recap's own window: resolved_at >= now - 24h
recap_since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
list_since = (datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)).isoformat()


def _parse(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc)

    # One row per free-play alert (no signal_results fan-out), with commence_time
    # and the count of matching signal_results rows (sr_count > 1 = the recap's
    # INNER JOIN would double-count this play once it grades).
    sql = """
        SELECT sa.event_id, sa.alert_type, sa.market_key, sa.outcome_name, sa.sent_at,
          sa.details_json,
          (SELECT o.away_team || ' @ ' || o.home_team FROM odds_snapshots o
             WHERE o.event_id = sa.event_id LIMIT 1) AS matchup,
          (SELECT MAX(o.commence_time) FROM odds_snapshots o
             WHERE o.event_id = sa.event_id) AS commence_time,
          (SELECT COUNT(*) FROM signal_results sr
             WHERE sr.event_id = sa.event_id AND sr.signal_type = sa.alert_type
               AND sr.market_key = sa.market_key AND sr.outcome_name = sa.outcome_name) AS sr_count,
          (SELECT sr.result FROM signal_results sr
             WHERE sr.event_id = sa.event_id AND sr.signal_type = sa.alert_type
               AND sr.market_key = sa.market_key AND sr.outcome_name = sa.outcome_name
               AND sr.result IS NOT NULL LIMIT 1) AS result,
          (SELECT MAX(sr.resolved_at) FROM signal_results sr
             WHERE sr.event_id = sa.event_id AND sr.signal_type = sa.alert_type
               AND sr.market_key = sa.market_key AND sr.outcome_name = sa.outcome_name) AS resolved_at
        FROM sent_alerts sa
        WHERE sa.is_free_play = 1 AND sa.sent_at >= ?
        ORDER BY sa.sent_at ASC
    """
    rows = [dict(r) for r in db.execute(sql, (list_since,))]
    db.close()

    print(f"Free plays in last {HOURS_BACK}h - DB: {DB_PATH}")
    print(f"Now: {now.isoformat()}    Recap window (resolved_at >=): {recap_since}\n")

    if not rows:
        print("No free plays in window.")
        return

    in_recap = 0
    wins = losses = pushes = 0
    fanout = 0
    print("  {:<24} {:<7} {:<6} {:>5} {:<12} {:<12} {:<4} {:<6} {}".format(
        "matchup", "outcome", "market", "line", "sent", "commence", "sr#", "result", "recap status"))
    print("  " + "-" * 120)
    for r in rows:
        sr_count = r["sr_count"] or 0
        try:
            det = json.loads(r["details_json"]) if r["details_json"] else {}
        except (ValueError, TypeError):
            det = {}
        line = det.get("us_value")
        if line is None:
            vbs = det.get("value_books") or []
            if vbs and isinstance(vbs[0], dict):
                line = vbs[0].get("point")
        result = r["result"]
        resolved_at = r["resolved_at"]
        commence = _parse(r["commence_time"])
        survives = bool(sr_count >= 1 and resolved_at is not None and resolved_at >= recap_since)
        if survives:
            in_recap += 1
            if result == "won":
                wins += 1
            elif result == "lost":
                losses += 1
            elif result == "push":
                pushes += 1
        if sr_count > 1:
            fanout += 1

        if sr_count == 0:
            why = "NO signal_results match (dropped even after grading)"
        elif resolved_at is not None and resolved_at >= recap_since:
            why = "IN RECAP" + ("  [DOUBLE-COUNT: sr#>1]" if sr_count > 1 else "")
        elif resolved_at is not None:
            why = "graded before 24h window"
        elif commence is not None and commence > now:
            why = "not yet played (game in future) - legit ungraded"
        elif commence is not None:
            why = ">>> GAME FINISHED but UNGRADED - grading gap"
        else:
            why = "UNGRADED, commence_time unknown"

        print("  {:<24} {:<7} {:<6} {:>5} {:<12} {:<12} {:<4} {:<6} {}".format(
            (r["matchup"] or r["event_id"] or "")[:24], (r["outcome_name"] or "")[:7],
            (r["market_key"] or "")[:6], str(line), str(r["sent_at"])[5:16],
            str(r["commence_time"])[5:16], sr_count, str(result), why))

    print(f"\n  Free-play alerts in window: {len(rows)}   |   Survive recap join+window: {in_recap}")
    print(f"  Recap record would read: {wins}-{losses}" + (f" ({pushes} push)" if pushes else ""))
    dropped = len(rows) - in_recap
    if dropped:
        print(f"  {dropped} sent but NOT in recap — see 'recap status' column.")
    if fanout:
        print(f"  WARNING: {fanout} play(s) match >1 signal_results row — recap INNER JOIN "
              f"will COUNT THEM MULTIPLE TIMES once graded.")


if __name__ == "__main__":
    main()
