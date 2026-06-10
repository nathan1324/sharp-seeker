"""Why is the combined "Daily Signal Report" dropping graded plays? (read-only)

The combined recap historically counted only plays it considered "sent": the
filter was qualifier_count>0, windowed by signal_at. Two ways a genuinely
sent + graded play fell out:

  1. RAW-PD SEND: PD signals for MLB/WNBA route to a dedicated raw channel and
     are stored qualifier_count=0 (they bypass the qualifier gate) -- but they
     ARE published to Discord and recorded in sent_alerts. The qualifier>0
     proxy dropped every one of them.
  2. EARLY FIRE: a play that fired >24h before its game was graded had a
     signal_at outside the recap's 24h window (MLB lines post early).

The fix: "sent" = a sent_alerts row exists (ground truth), windowed by
resolved_at (grading time). This script replays one recap cycle on the live DB
and reports, per play graded in the window, whether the OLD filter dropped it
and WHY -- with record + units, split by sport x market.

Read-only. Streams rows (server has ~954MB RAM).

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/diagnose_combined_recap.py [hours] [db_path]
  (defaults: hours=24, db_path=/app/data/sharp_seeker.db)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

HOURS = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 24
DB_PATH = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "/app/data/sharp_seeker.db"


def _units(details_json, result, signal_type=None):
    """Risk-adjusted units (mirror of reports._units_from_signal)."""
    d = None
    if details_json:
        try:
            d = json.loads(details_json) if isinstance(details_json, str) else details_json
        except (json.JSONDecodeError, TypeError, AttributeError):
            d = None
    # Arbitrage: count its guaranteed profit_pct (~0 impact), not the side-A swing.
    if signal_type == "arbitrage":
        pct = d.get("profit_pct") if d else None
        return round(pct / 100.0, 4) if pct is not None else 0.0
    if result == "push":
        return 0.0
    price = None
    qcount = 0
    if d:
        vb = d.get("value_books", [])
        if vb:
            price = vb[0].get("price")
        qcount = d.get("qualifier_count", 0)
    if price is None:
        return 0.0
    mult = 2 if qcount >= 2 else 1
    risk = abs(price) / 100.0 if price < 0 else 100.0 / price
    if result == "won":
        return 1.0 * mult
    if result == "lost":
        return -risk * mult
    return 0.0


def _qcount(details_json):
    if not details_json:
        return 0
    try:
        d = json.loads(details_json) if isinstance(details_json, str) else details_json
        return d.get("qualifier_count", 0)
    except (json.JSONDecodeError, TypeError, AttributeError):
        return 0


def _tally(bucket, key, result, u):
    agg = bucket.setdefault(key, [0, 0.0, 0, 0])
    agg[0] += 1
    agg[1] += u
    if result == "won":
        agg[2] += 1
    elif result == "lost":
        agg[3] += 1


def _print_breakdown(title, bucket):
    if not bucket:
        return
    print(title)
    for key, v in sorted(bucket.items(), key=lambda kv: kv[1][0], reverse=True):
        cnt, uu, w, l = v
        label = key if isinstance(key, str) else (key[0] + " " + key[1])
        print("  " + label + ": " + str(cnt) + " plays  "
              + str(w) + "W/" + str(l) + "L  " + format(uu, "+.2f") + "u")


def main():
    now = datetime.now(timezone.utc)
    recap_since = (now - timedelta(hours=HOURS)).isoformat()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Every play graded in this recap cycle, with whether it was actually sent
    # to Discord (a sent_alerts row exists).
    sql = """
        SELECT sr.sport_key, sr.signal_type, sr.market_key, sr.result,
               sr.signal_at, sr.resolved_at, sr.details_json,
               EXISTS (SELECT 1 FROM sent_alerts sa
                       WHERE sa.event_id = sr.event_id
                         AND sa.alert_type = sr.signal_type
                         AND sa.market_key = sr.market_key
                         AND sa.outcome_name = sr.outcome_name) AS was_sent
        FROM signal_results sr
        WHERE sr.result IS NOT NULL
          AND sr.resolved_at >= ?
        ORDER BY sr.resolved_at ASC
    """
    cur = conn.execute(sql, (recap_since,))

    new_in = 0
    new_units = new_w = new_l = 0
    old_in = 0
    recovered = 0
    recovered_units = 0.0
    by_reason = {}      # reason -> [cnt, units, w, l]
    by_combo = {}       # (sport, market) -> [...]  (recovered only)
    suppressed = 0      # graded but never sent (correctly excluded by both)
    arbs_skipped = 0    # excluded from the recap by design (guaranteed-profit)

    for row in cur:
        # Arbitrage is excluded from the recap entirely (guaranteed-profit, not a
        # directional play) — skip so these numbers match what the recap shows.
        if row["signal_type"] == "arbitrage":
            arbs_skipped += 1
            continue

        result = row["result"]
        u = _units(row["details_json"], result, row["signal_type"])
        q = _qcount(row["details_json"])
        was_sent = bool(row["was_sent"])
        signal_at = row["signal_at"] or ""

        if not was_sent:
            suppressed += 1
            continue

        # NEW combined recap: sent + graded in window (selection guarantees window).
        new_in += 1
        new_units += u
        if result == "won":
            new_w += 1
        elif result == "lost":
            new_l += 1

        # OLD combined recap: qualifier>0 AND signal_at in 24h window.
        old_qualified = q > 0
        old_windowed = signal_at >= recap_since
        if old_qualified and old_windowed:
            old_in += 1
            continue

        # Sent + graded but the OLD recap dropped it -> recovered by the fix.
        recovered += 1
        recovered_units += u
        if not old_qualified and not old_windowed:
            reason = "raw-PD send AND fired >window"
        elif not old_qualified:
            reason = "raw-PD send (qualifier=0)"
        else:
            reason = "fired before 24h window"
        _tally(by_reason, reason, result, u)
        _tally(by_combo, (row["sport_key"] or "?", row["market_key"] or "?"), result, u)

    conn.close()

    print("Combined-recap diagnostic - DB: " + DB_PATH)
    print("Recap cycle: last " + str(HOURS) + "h of gradings (resolved_at >= "
          + recap_since[:16] + ")\n")
    print("Sent + graded in window (NEW recap shows): " + str(new_in)
          + "  -> " + str(new_w) + "W/" + str(new_l) + "L  "
          + format(new_units, "+.2f") + "u")
    print("  Already shown by OLD recap:              " + str(old_in))
    print("  RECOVERED by the fix:                    " + str(recovered)
          + "  -> " + format(recovered_units, "+.2f") + "u")
    print("Graded but never sent (excluded by both):  " + str(suppressed))
    print("Arbitrage (excluded from recap by design):  " + str(arbs_skipped) + "\n")
    if recovered:
        _print_breakdown("Recovered, by reason:", by_reason)
        print("")
        _print_breakdown("Recovered, by sport x market:", by_combo)
    else:
        print("No sent+graded plays were dropped by the old recap this cycle.")


if __name__ == "__main__":
    main()
