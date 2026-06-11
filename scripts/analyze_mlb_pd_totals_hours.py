"""Where does MLB PD totals actually win? By-hour edge analysis (read-only).

MLB Pinnacle-Divergence totals are gated out of X free plays (and the qualified
Discord recap) because they earn 0 qualifiers: MLB has no totals best-combo and
its best-hours list is [] ("no data yet", set 2026-03-31). We now have data, so
this script breaks the market down by MST hour (the unit best-hours operates on)
to see whether a profitable slice exists worth assigning as best hours.

Scope: defaults to the last 50 days so it reflects the CURRENT book regime
(DraftKings was excluded from MLB PD on 2026-04-25; older data is full of the
DK-driven losses and would mislead the by-hour read). Override with [days].

Counts only signals actually SENT to Discord (a sent_alerts row exists). This is
the decision-relevant population: quiet-hours and other pipeline filters drop
signals that are still recorded in signal_results for analysis but never reach
Discord, so a best-hour tag on those hours would be dead weight. Filtering to
sent-only excludes them via ground truth, without re-deriving the quiet-hours
config. Pass "all" as the 3rd arg to include unsent (recorded-only) signals too.

Dedups by (event_id, outcome_name) keeping the latest signal_at, mirroring the
recap/stats queries. Read-only; streams rows (server has ~954MB RAM).

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_mlb_pd_totals_hours.py [days] [db_path]
  (defaults: days=50, db_path=/app/data/sharp_seeker.db)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 50
DB_PATH = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "/app/data/sharp_seeker.db"
# Pass "all" as the 3rd arg to include recorded-but-unsent signals; default is
# sent-to-Discord only (the population a best-hour qualifier could actually act on).
SENT_ONLY = not (len(sys.argv) > 3 and sys.argv[3] == "all")

# A slice must clear all three to be suggested as a best hour.
MIN_N = 6
MIN_WR = 0.55
MST = timezone(timedelta(hours=-7))  # America/Phoenix, no DST
RECENT_DAYS = 14  # trailing window for the trend split


def _units(details_json, result):
    """Risk-adjusted units at 1u flat (a promoted MLB play would be 1-qualifier,
    so no Elite 2x). won at -110 = +1.0; lost at -110 = -1.1; push/no-price = 0."""
    if result == "push":
        return 0.0
    price = None
    if details_json:
        try:
            d = json.loads(details_json) if isinstance(details_json, str) else details_json
            vb = d.get("value_books", [])
            if vb:
                price = vb[0].get("price")
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass
    if price is None:
        return 0.0
    risk = abs(price) / 100.0 if price < 0 else 100.0 / price
    return 1.0 if result == "won" else -risk


def _mst_hour(ts):
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MST).hour


def _new_bucket():
    return {"n": 0, "w": 0, "l": 0, "p": 0, "u": 0.0}


def _add(b, result, u):
    b["n"] += 1
    b["u"] += u
    if result == "won":
        b["w"] += 1
    elif result == "lost":
        b["l"] += 1
    else:
        b["p"] += 1


def _wr(b):
    decided = b["w"] + b["l"]
    return (b["w"] / decided) if decided else 0.0


def _fmt(b):
    decided = b["w"] + b["l"]
    wr = ("{:.0%}".format(b["w"] / decided)) if decided else "  -"
    return (str(b["n"]).rjust(4) + "   " + (str(b["w"]) + "-" + str(b["l"]) + "-" + str(b["p"])).ljust(9)
            + "  WR " + wr.rjust(4) + "   " + format(b["u"], "+.2f").rjust(8) + "u")


def main():
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=DAYS)).isoformat()
    recent_cut = (now - timedelta(days=RECENT_DAYS)).isoformat()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Resolved MLB PD totals in the window, deduped to the latest fire per play.
    # When SENT_ONLY, restrict to plays actually published to Discord.
    sent_clause = ""
    if SENT_ONLY:
        sent_clause = (
            " AND EXISTS (SELECT 1 FROM sent_alerts sa"
            " WHERE sa.event_id = signal_results.event_id"
            " AND sa.alert_type = 'pinnacle_divergence'"
            " AND sa.market_key = 'totals'"
            " AND sa.outcome_name = signal_results.outcome_name)"
        )
    sql = """
        SELECT signal_at, result, signal_strength, details_json
        FROM (
            SELECT signal_at, result, signal_strength, details_json,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id, outcome_name
                       ORDER BY signal_at DESC
                   ) AS rn
            FROM signal_results
            WHERE result IS NOT NULL
              AND sport_key = 'baseball_mlb'
              AND signal_type = 'pinnacle_divergence'
              AND market_key = 'totals'
              AND signal_at >= ?{sent}
        )
        WHERE rn = 1
        ORDER BY signal_at ASC
    """.format(sent=sent_clause)
    cur = conn.execute(sql, (since,))

    overall = _new_bucket()
    recent = _new_bucket()
    older = _new_bucket()
    by_hour = {h: _new_bucket() for h in range(24)}
    strength_buckets = {"<0.30": _new_bucket(), "0.30-0.50": _new_bucket(),
                        "0.50-0.70": _new_bucket(), ">=0.70": _new_bucket()}

    for row in cur:
        result = row["result"]
        u = _units(row["details_json"], result)
        _add(overall, result, u)
        if row["signal_at"] >= recent_cut:
            _add(recent, result, u)
        else:
            _add(older, result, u)
        h = _mst_hour(row["signal_at"])
        if h is not None:
            _add(by_hour[h], result, u)
        s = row["signal_strength"]
        if s is None:
            key = "<0.30"
        elif s < 0.30:
            key = "<0.30"
        elif s < 0.50:
            key = "0.30-0.50"
        elif s < 0.70:
            key = "0.50-0.70"
        else:
            key = ">=0.70"
        _add(strength_buckets[key], result, u)

    conn.close()

    scope = "SENT to Discord only" if SENT_ONLY else "ALL recorded (incl. unsent)"
    print("MLB PD totals by-hour analysis - DB: " + DB_PATH)
    print("Window: last " + str(DAYS) + " days (since " + since[:10]
          + "); " + scope + "; dedup latest fire per play\n")
    print("OVERALL: " + _fmt(overall))
    print("  last " + str(RECENT_DAYS) + "d:  " + _fmt(recent))
    print("  earlier:   " + _fmt(older) + "\n")

    print("By MST hour (signal fire time):")
    print("  hr     n   W-L-P       WR        units   flag")
    suggested = []
    for h in range(24):
        b = by_hour[h]
        if b["n"] == 0:
            continue
        flag = ""
        if b["n"] >= MIN_N and _wr(b) >= MIN_WR and b["u"] > 0:
            flag = "  <-- clears bar"
            suggested.append(h)
        print("  " + str(h).rjust(2) + "   " + _fmt(b) + flag)

    print("\nBy strength bucket:")
    for key in ["<0.30", "0.30-0.50", "0.50-0.70", ">=0.70"]:
        b = strength_buckets[key]
        if b["n"]:
            print("  " + key.ljust(10) + " " + _fmt(b))

    print("\nBar: n >= " + str(MIN_N) + ", WR >= " + format(MIN_WR, ".0%") + ", units > 0")
    if suggested:
        hours_json = "[" + ", ".join(str(h) for h in suggested) + "]"
        print("Suggested MLB best hours (MST): " + hours_json)
        print('  -> SIGNAL_BEST_HOURS key "pinnacle_divergence:baseball_mlb": ' + hours_json)
    else:
        print("No hour clears the bar -- do NOT assign MLB best hours yet.")


if __name__ == "__main__":
    main()
