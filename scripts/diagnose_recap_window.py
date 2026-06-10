"""Quantify daily-recap signals dropped by signal_at windowing (read-only).

The Discord daily recap historically windowed performance by signal_at (when a
signal FIRED) instead of resolved_at (when it was GRADED). The recap runs right
after the daily grading pass, so a play that fired more than ~24h before its
game was graded fell outside the recap's 24h signal_at window and was silently
dropped from the report — even though it was freshly graded.

This script replays that gap on the live DB: for every signal graded in the last
N days, it flags the ones whose signal_at was more than 24h before their
resolved_at (i.e. would have been missed by the old recap) and tallies the
record + unit impact, overall and split by sport x market.

Read-only. Streams rows (server has ~954MB RAM — no fetchall of the whole table).

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/diagnose_recap_window.py [days] [db_path]
  (defaults: days=30, db_path=/app/data/sharp_seeker.db)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 30
DB_PATH = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "/app/data/sharp_seeker.db"

# A play is "missed" if it fired more than this long before it was graded. The
# recap runs ~15 min after grading with a 24h lookback, so 24h is a slightly
# conservative proxy (it undercounts borderline plays rather than over-claiming).
MISS_THRESHOLD = timedelta(hours=24)


def _units(details_json, result):
    """Risk-adjusted units for one resolved play (mirror of reports._compute_units)."""
    if result == "push":
        return 0.0
    price = None
    qcount = 0
    if details_json:
        try:
            d = json.loads(details_json) if isinstance(details_json, str) else details_json
            vb = d.get("value_books", [])
            if vb:
                price = vb[0].get("price")
            qcount = d.get("qualifier_count", 0)
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass
    if price is None:
        return 0.0
    mult = 2 if qcount >= 2 else 1
    if price < 0:
        risk = abs(price) / 100.0
    else:
        risk = 100.0 / price
    if result == "won":
        return 1.0 * mult
    if result == "lost":
        return -risk * mult
    return 0.0


def _parse(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def main():
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=DAYS)).isoformat()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Only graded plays in the window; sent-to-Discord only (qualifier>0), since
    # that's what the main recap reports. Stream — do not fetchall.
    sql = """
        SELECT sport_key, signal_type, market_key, result,
               signal_at, resolved_at, details_json
        FROM signal_results
        WHERE result IS NOT NULL
          AND resolved_at >= ?
          AND (COALESCE(json_extract(details_json, '$.qualifier_count'), 1) > 0
               OR signal_type = 'arbitrage')
        ORDER BY resolved_at ASC
    """
    cur = conn.execute(sql, (since,))

    tot_graded = 0
    tot_missed = 0
    missed_w = missed_l = missed_p = 0
    missed_units = 0.0
    by_combo = {}  # (sport, market) -> [missed_count, missed_units, missed_w, missed_l]
    max_gap_h = 0.0

    for row in cur:
        tot_graded += 1
        sa = _parse(row["signal_at"])
        ra = _parse(row["resolved_at"])
        if sa is None or ra is None:
            continue
        gap = ra - sa
        if gap <= MISS_THRESHOLD:
            continue
        # This play would have been dropped by the old signal_at-windowed recap.
        tot_missed += 1
        result = row["result"]
        u = _units(row["details_json"], result)
        missed_units += u
        if result == "won":
            missed_w += 1
        elif result == "lost":
            missed_l += 1
        else:
            missed_p += 1
        gap_h = gap.total_seconds() / 3600.0
        if gap_h > max_gap_h:
            max_gap_h = gap_h
        key = (row["sport_key"] or "?", row["market_key"] or "?")
        agg = by_combo.setdefault(key, [0, 0.0, 0, 0])
        agg[0] += 1
        agg[1] += u
        if result == "won":
            agg[2] += 1
        elif result == "lost":
            agg[3] += 1

    conn.close()

    pct = (100.0 * tot_missed / tot_graded) if tot_graded else 0.0
    print("Recap window diagnostic - DB: " + DB_PATH)
    print("Window: last " + str(DAYS) + " days of gradings (since " + since[:16] + ")")
    print("Miss rule: signal_at more than 24h before resolved_at (sent-to-Discord plays)\n")
    print("Graded plays in window:        " + str(tot_graded))
    print("Dropped by old signal_at recap: " + str(tot_missed) + "  (" + format(pct, ".1f") + "%)")
    if tot_missed:
        print("  Missed record:  " + str(missed_w) + "W / " + str(missed_l) + "L / " + str(missed_p) + "P")
        print("  Missed units:   " + format(missed_units, "+.2f") + "u")
        print("  Largest fire->grade gap: " + format(max_gap_h, ".1f") + "h\n")
        print("By sport x market (missed only):")
        for (sport, market), v in sorted(by_combo.items(), key=lambda kv: kv[1][0], reverse=True):
            cnt, uu, w, l = v
            print("  " + sport + " " + market + ": " + str(cnt)
                  + " plays  " + str(w) + "W/" + str(l) + "L  " + format(uu, "+.2f") + "u")
    else:
        print("\nNo graded plays fired >24h before grading in this window —")
        print("the recap window had no measurable gap over the period.")


if __name__ == "__main__":
    main()
