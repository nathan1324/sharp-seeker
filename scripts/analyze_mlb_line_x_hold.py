"""Line x hold cross-tab for MLB PD totals: independent levers or one? (read-only)

The strength-meaning pass found two live dimensions in the constant-strength MLB
PD-totals population: the TOTAL LINE (8.0-8.5 great, 9.0-9.5 a hole) and
CROSS-BOOK HOLD (2%+ beats negative). They might be the same lever twice -- the
9.0-9.5 hole could just BE the negative-hold plays, since 9.0/9.5 are MLB's most
common, most efficiently priced totals. This cross-tabs the two so we can tell:

  - If hold still splits WR/units WITHIN each line band (and line still splits
    within each hold band), they are INDEPENDENT levers -> a real strength
    blends both.
  - If the hole vanishes once you hold one fixed, it was confounded -> keep only
    the dominant lever and don't double-count it.

Reads the joint cell counts plus both sets of marginals. Sent-only, windowed by
resolved_at, dedup latest fire -- same population as analyze_mlb_strength_meaning.
Read-only; streams rows (server has ~954MB RAM).

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_mlb_line_x_hold.py [days|since-date] [db_path] [sport]
  (defaults: 60 days, db=/app/data/sharp_seeker.db, sport=baseball_mlb; "all" for every sport)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

ARG1 = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else "60"
DB_PATH = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "/app/data/sharp_seeker.db"
SPORT = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else "baseball_mlb"

LINE_BANDS = ["<= 7.5", "8.0-8.5", "9.0-9.5", ">= 10", "no line"]
HOLD_BANDS = ["neg", "0-2%", "2%+", "no hold"]


def _since():
    if "-" in ARG1:
        return ARG1 if "T" in ARG1 else ARG1 + "T00:00:00+00:00"
    return (datetime.now(timezone.utc) - timedelta(days=int(ARG1))).isoformat()


def _details(details_json):
    if not details_json:
        return {}
    try:
        d = json.loads(details_json) if isinstance(details_json, str) else details_json
        return d if isinstance(d, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _price(d):
    vb = d.get("value_books", [])
    return vb[0].get("price") if vb else None


def _risk(price):
    return abs(price) / 100.0 if price < 0 else 100.0 / price


def _be(price):
    return abs(price) / (abs(price) + 100.0) if price < 0 else 100.0 / (price + 100.0)


def _line_band(us_value):
    if us_value is None:
        return "no line"
    if us_value <= 7.5:
        return "<= 7.5"
    if us_value <= 8.5:
        return "8.0-8.5"
    if us_value <= 9.5:
        return "9.0-9.5"
    return ">= 10"


def _hold_band(h):
    if h is None:
        return "no hold"
    if h < 0:
        return "neg"
    if h < 0.02:
        return "0-2%"
    return "2%+"


def _new():
    return {"n": 0, "w": 0, "l": 0, "p": 0, "u": 0.0, "be_sum": 0.0, "be_n": 0}


def _add(b, result, price):
    b["n"] += 1
    if result == "won":
        b["w"] += 1
    elif result == "lost":
        b["l"] += 1
    else:
        b["p"] += 1
    if price is None:
        return
    b["be_sum"] += _be(price)
    b["be_n"] += 1
    if result == "won":
        b["u"] += 1.0
    elif result == "lost":
        b["u"] -= _risk(price)


def _cell(b):
    """Compact cell: 'n WR% +u' or '.' when empty. Fixed width 16."""
    if b["n"] == 0:
        return ".".center(16)
    d = b["w"] + b["l"]
    wr = ("{:.0%}".format(b["w"] / d)) if d else "-"
    txt = str(b["n"]) + " " + wr + " " + format(b["u"], "+.1f")
    return txt.center(16)


def _line(b):
    """One-line summary for a marginal bucket."""
    d = b["w"] + b["l"]
    wr = ("{:.0%}".format(b["w"] / d)) if d else "  -"
    be = ("{:.0%}".format(b["be_sum"] / b["be_n"])) if b["be_n"] else "  -"
    edge = ("{:+.1f}pp".format(100.0 * (b["w"] / d - b["be_sum"] / b["be_n"]))
            if d and b["be_n"] else "   -")
    rec = str(b["w"]) + "-" + str(b["l"]) + "-" + str(b["p"])
    return (str(b["n"]).rjust(4) + "  " + rec.ljust(10)
            + " WR " + wr.rjust(4) + "  BE " + be.rjust(4)
            + "  edge " + edge.rjust(7) + "  units " + format(b["u"], "+.2f").rjust(8))


def main():
    since = _since()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    sport_clause = "" if SPORT == "all" else " AND sport_key = ?"
    params = [since] + ([] if SPORT == "all" else [SPORT])
    sql = """
        SELECT result, details_json
        FROM (
            SELECT result, details_json, event_id, outcome_name,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id, outcome_name ORDER BY signal_at DESC
                   ) AS rn
            FROM signal_results
            WHERE result IS NOT NULL
              AND signal_type = 'pinnacle_divergence'
              AND market_key = 'totals'
              AND resolved_at >= ?{sport}
              AND EXISTS (SELECT 1 FROM sent_alerts sa
                          WHERE sa.event_id = signal_results.event_id
                          AND sa.alert_type = 'pinnacle_divergence'
                          AND sa.market_key = 'totals'
                          AND sa.outcome_name = signal_results.outcome_name)
        )
        WHERE rn = 1
    """.format(sport=sport_clause)
    cur = conn.execute(sql, params)

    grid = {(lb, hb): _new() for lb in LINE_BANDS for hb in HOLD_BANDS}
    row_marg = {lb: _new() for lb in LINE_BANDS}
    col_marg = {hb: _new() for hb in HOLD_BANDS}
    overall = _new()

    for row in cur:
        d = _details(row["details_json"])
        result = row["result"]
        price = _price(d)
        lb = _line_band(d.get("us_value"))
        hb = _hold_band(d.get("cross_book_hold"))
        _add(grid[(lb, hb)], result, price)
        _add(row_marg[lb], result, price)
        _add(col_marg[hb], result, price)
        _add(overall, result, price)

    conn.close()

    scope = SPORT if SPORT != "all" else "all sports"
    print("MLB PD-totals  LINE x HOLD cross-tab (" + scope + ") - DB: " + DB_PATH)
    print("Since " + since[:10] + " (by resolved_at); sent-only; dedup latest fire")
    print("\nOVERALL: " + _line(overall))

    # Only show hold columns that actually have data, plus a row total.
    active_holds = [hb for hb in HOLD_BANDS if col_marg[hb]["n"] > 0]
    active_lines = [lb for lb in LINE_BANDS if row_marg[lb]["n"] > 0]

    print("\n=== JOINT CELLS  (cell = n  WR  +units) ===")
    header = "line \\ hold ".ljust(12)
    for hb in active_holds:
        header += hb.center(16)
    header += "row total".center(16)
    print(header)
    for lb in active_lines:
        line = lb.ljust(12)
        for hb in active_holds:
            line += _cell(grid[(lb, hb)])
        line += _cell(row_marg[lb])
        print(line)
    footer = "col total".ljust(12)
    for hb in active_holds:
        footer += _cell(col_marg[hb])
    footer += _cell(overall)
    print(footer)

    print("\n=== HOLD WITHIN EACH LINE BAND (does hold still split?) ===")
    for lb in active_lines:
        print("  " + lb + ":")
        for hb in active_holds:
            b = grid[(lb, hb)]
            if b["n"]:
                print("    hold " + hb.ljust(6) + " " + _line(b))

    print("\n=== LINE WITHIN EACH HOLD BAND (does line still split?) ===")
    for hb in active_holds:
        print("  hold " + hb + ":")
        for lb in active_lines:
            b = grid[(lb, hb)]
            if b["n"]:
                print("    " + lb.ljust(8) + " " + _line(b))

    print("\nRead: if the 9.0-9.5 hole stays negative across BOTH hold columns, line")
    print("is its own lever. If 9.0-9.5 is only bad in 'neg' hold (and fine at 2%+),")
    print("the hole was confounded with hold -> blend on hold, not the raw line.")
    print("Watch cell n: anything below ~15 is suggestive only, not conclusive.")


if __name__ == "__main__":
    main()
