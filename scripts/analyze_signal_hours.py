"""By-hour edge analysis for any signal type (read-only).

Generalizes the MLB PD-totals analysis: given a signal type, breaks its
sent-to-Discord plays down by MST fire hour and compares the data-suggested
best hours to what's CURRENTLY configured in SIGNAL_BEST_HOURS. Use it to
confirm a type's best-hours list still matches reality.

Counts only signals SENT to Discord (a sent_alerts row exists) -- the population
a best-hour qualifier actually acts on. Quiet-hours/other-filter drops are
recorded in signal_results but never sent, so sent-only excludes them via ground
truth. Pass "all" as 4th arg to include recorded-but-unsent signals.

Dedups by (event_id, market_key, outcome_name) keeping the latest signal_at.
Read-only; streams rows (server has ~954MB RAM).

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_signal_hours.py <signal_type> [days] [db_path] [all]
  e.g.  ... analyze_signal_hours.py steam_move 60
  (defaults: days=60, db_path=/app/data/sharp_seeker.db, sent-only)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

if len(sys.argv) < 2 or not sys.argv[1]:
    print("usage: analyze_signal_hours.py <signal_type> [days] [db_path] [all]")
    raise SystemExit(2)

SIGNAL_TYPE = sys.argv[1]
DAYS = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else 60
DB_PATH = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else "/app/data/sharp_seeker.db"
SENT_ONLY = "all" not in sys.argv[2:]

MIN_N = 6
MIN_WR = 0.55
MST = timezone(timedelta(hours=-7))  # America/Phoenix, no DST
RECENT_DAYS = 14


def _units(details_json, result):
    """Risk-adjusted units at 1u flat. won at -110 = +1.0; lost = -1.1; push = 0."""
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


def _new():
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
    return (str(b["n"]).rjust(4) + "   "
            + (str(b["w"]) + "-" + str(b["l"]) + "-" + str(b["p"])).ljust(9)
            + "  WR " + wr.rjust(4) + "   " + format(b["u"], "+.2f").rjust(8) + "u")


def _current_best_hours():
    """Read the currently-configured best hours for this signal type from the
    live settings (so config-vs-data is a direct comparison). Returns None if
    settings can't be loaded."""
    try:
        from sharp_seeker.config import Settings
        bh = Settings().signal_best_hours or {}
        return bh.get(SIGNAL_TYPE)
    except Exception as exc:  # noqa: BLE001 - best-effort, never block the report
        print("  (could not load live config: " + str(exc) + ")")
        return None


def main():
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=DAYS)).isoformat()
    recent_cut = (now - timedelta(days=RECENT_DAYS)).isoformat()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    sent_clause = ""
    if SENT_ONLY:
        sent_clause = (
            " AND EXISTS (SELECT 1 FROM sent_alerts sa"
            " WHERE sa.event_id = signal_results.event_id"
            " AND sa.alert_type = signal_results.signal_type"
            " AND sa.market_key = signal_results.market_key"
            " AND sa.outcome_name = signal_results.outcome_name)"
        )
    sql = """
        SELECT signal_at, result, details_json, market_key, sport_key
        FROM (
            SELECT signal_at, result, details_json, market_key, sport_key, event_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id, market_key, outcome_name
                       ORDER BY signal_at DESC
                   ) AS rn
            FROM signal_results
            WHERE result IS NOT NULL
              AND signal_type = ?
              AND signal_at >= ?{sent}
        )
        WHERE rn = 1
        ORDER BY signal_at ASC
    """.format(sent=sent_clause)
    cur = conn.execute(sql, (SIGNAL_TYPE, since))

    overall = _new()
    recent = _new()
    by_hour = {h: _new() for h in range(24)}
    by_market = {}
    by_sport = {}

    for row in cur:
        result = row["result"]
        u = _units(row["details_json"], result)
        _add(overall, result, u)
        if row["signal_at"] >= recent_cut:
            _add(recent, result, u)
        h = _mst_hour(row["signal_at"])
        if h is not None:
            _add(by_hour[h], result, u)
        _add(by_market.setdefault(row["market_key"] or "?", _new()), result, u)
        _add(by_sport.setdefault(row["sport_key"] or "?", _new()), result, u)

    conn.close()

    scope = "SENT to Discord only" if SENT_ONLY else "ALL recorded (incl. unsent)"
    print(SIGNAL_TYPE + " by-hour analysis - DB: " + DB_PATH)
    print("Window: last " + str(DAYS) + " days (since " + since[:10]
          + "); " + scope + "; dedup latest fire per play\n")
    print("OVERALL: " + _fmt(overall))
    print("  last " + str(RECENT_DAYS) + "d:  " + _fmt(recent) + "\n")

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

    print("\nBy market:")
    for mk, b in sorted(by_market.items(), key=lambda kv: kv[1]["u"], reverse=True):
        print("  " + mk.ljust(10) + " " + _fmt(b))
    print("\nBy sport:")
    for sk, b in sorted(by_sport.items(), key=lambda kv: kv[1]["u"], reverse=True):
        print("  " + sk.ljust(18) + " " + _fmt(b))

    print("\nBar: n >= " + str(MIN_N) + ", WR >= " + format(MIN_WR, ".0%") + ", units > 0")
    current = _current_best_hours()
    print("Currently configured best hours: "
          + (str(current) if current is not None else "(unknown)"))
    print("Data-suggested best hours:       " + str(suggested))
    if current is not None:
        cur_set, sug_set = set(current), set(suggested)
        dropped = sorted(cur_set - sug_set)
        added = sorted(sug_set - cur_set)
        if dropped:
            print("  configured but NOT clearing bar (consider removing): " + str(dropped))
        if added:
            print("  clears bar but NOT configured (consider adding):     " + str(added))
        if not dropped and not added:
            print("  config matches the data.")


if __name__ == "__main__":
    main()
