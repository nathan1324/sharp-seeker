"""By-combo (sport x market) edge analysis for any signal type (read-only).

Companion to analyze_signal_hours.py. Where the hours script asks "which hours
win", this asks "which sport x market combos win" — the intersection that
best-hours (a global lever) can't separate. Use it to decide which
`signal_type:sport:market` entries belong in SIGNAL_BEST_COMBOS.

Counts only signals SENT to Discord (a sent_alerts row exists) and dedups by
(event_id, market_key, outcome_name) keeping the latest fire. Prints each combo's
record + units, marks which are CURRENTLY configured best combos, and flags
configured-but-losing / strong-but-unconfigured. Read-only; streams rows.

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_signal_combos.py <signal_type> [days] [db_path] [all]
  e.g.  ... analyze_signal_combos.py steam_move 60
  (defaults: days=60, db_path=/app/data/sharp_seeker.db, sent-only)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

if len(sys.argv) < 2 or not sys.argv[1]:
    print("usage: analyze_signal_combos.py <signal_type> [days] [db_path] [all]")
    raise SystemExit(2)

SIGNAL_TYPE = sys.argv[1]
DAYS = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else 60
DB_PATH = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else "/app/data/sharp_seeker.db"
SENT_ONLY = "all" not in sys.argv[2:]

# Bars for the add/remove hints (combos carry more volume than single hours).
ADD_MIN_N = 15
ADD_MIN_WR = 0.55
RECENT_DAYS = 14


def _units(details_json, result):
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


def _new():
    return {"n": 0, "w": 0, "l": 0, "p": 0, "u": 0.0, "ru": 0.0, "rn": 0}


def _add(b, result, u, recent):
    b["n"] += 1
    b["u"] += u
    if recent:
        b["rn"] += 1
        b["ru"] += u
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
            + "  WR " + wr.rjust(4) + "   " + format(b["u"], "+.2f").rjust(8) + "u"
            + "   (last " + str(RECENT_DAYS) + "d " + format(b["ru"], "+.2f") + "u)")


def _configured_combos():
    try:
        from sharp_seeker.config import Settings
        return set(Settings().signal_best_combos or [])
    except Exception as exc:  # noqa: BLE001
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
        SELECT signal_at, result, details_json, sport_key, market_key
        FROM (
            SELECT signal_at, result, details_json, sport_key, market_key, event_id,
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

    by_combo = {}
    overall = _new()
    for row in cur:
        result = row["result"]
        u = _units(row["details_json"], result)
        recent = row["signal_at"] >= recent_cut
        key = (row["sport_key"] or "?", row["market_key"] or "?")
        _add(by_combo.setdefault(key, _new()), result, u, recent)
        _add(overall, result, u, recent)

    conn.close()

    scope = "SENT to Discord only" if SENT_ONLY else "ALL recorded (incl. unsent)"
    print(SIGNAL_TYPE + " by-combo analysis - DB: " + DB_PATH)
    print("Window: last " + str(DAYS) + " days (since " + since[:10]
          + "); " + scope + "; dedup latest fire per play\n")
    print("OVERALL: " + _fmt(overall) + "\n")

    configured = _configured_combos()
    print("Combo (signal_type:sport:market)              n   W-L-P       WR        units")
    remove_hints, add_hints = [], []
    for (sport, market), b in sorted(by_combo.items(), key=lambda kv: kv[1]["u"], reverse=True):
        combo = SIGNAL_TYPE + ":" + sport + ":" + market
        is_cfg = configured is not None and combo in configured
        mark = " [BEST COMBO]" if is_cfg else ""
        print("  " + combo.ljust(44) + " " + _fmt(b) + mark)
        if is_cfg and b["u"] < 0:
            remove_hints.append((combo, b["u"]))
        if (not is_cfg and b["n"] >= ADD_MIN_N and _wr(b) >= ADD_MIN_WR and b["u"] > 0):
            add_hints.append((combo, b["u"]))

    print("\nAdd bar: n >= " + str(ADD_MIN_N) + ", WR >= " + format(ADD_MIN_WR, ".0%")
          + ", units > 0")
    if configured is None:
        print("(live best-combos config unavailable — showing performance only)")
    if remove_hints:
        print("Configured best combos that are LOSING (consider removing):")
        for combo, u in remove_hints:
            print("  " + combo + "  " + format(u, "+.2f") + "u")
    if add_hints:
        print("Unconfigured combos clearing the add bar (consider adding):")
        for combo, u in add_hints:
            print("  " + combo + "  " + format(u, "+.2f") + "u")
    if configured is not None and not remove_hints and not add_hints:
        print("No add/remove changes suggested.")


if __name__ == "__main__":
    main()
