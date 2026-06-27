"""Why is MLB PD strength a dead constant, and what SHOULD strength mean? (read-only)

The reported `strength` for PD signals is `min(1.0, delta / (threshold * 3))`
(see engine/pinnacle_divergence.py:230; hold_boost is dead, stored 0.0). MLB
totals fire at the 0.5-run threshold and books ~never diverge a full run, so
delta is pinned at 0.5 -> strength = 0.5 / (0.5*3) = 0.333 on EVERY MLB totals
signal. A constant carries zero information: you cannot rank, gate, or size on
it. This script proves that, then hunts for the dimensions that DO vary across
the constant-strength MLB population and actually predict win rate -- the raw
material for a meaningful MLB-specific strength.

Part 1  PROOF      strength + delta distribution (expect ~1 value each)
Part 2  CANDIDATES  WR / units / break-even by dimensions that vary even when
                    delta is pinned:
                      - side (Over vs Under)
                      - total line band (us_value: low- vs high-scoring games)
                      - cross-book hold bucket (sharper market = ?)
                      - value-book price band (the juice we actually take)
                      - US-book dispersion bucket (is the value book a real outlier?)

Sent-only (a sent_alerts row exists), windowed by resolved_at, dedup latest fire
per (event, side) -- same population as the recap/best-hours/cushion queries.
Read-only; streams rows (server has ~954MB RAM).

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_mlb_strength_meaning.py [days|since-date] [db_path] [sport]
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


def _fmt(b):
    d = b["w"] + b["l"]
    wr = ("{:.0%}".format(b["w"] / d)) if d else "  -"
    be = ("{:.0%}".format(b["be_sum"] / b["be_n"])) if b["be_n"] else "  -"
    edge = ("{:+.1f}pp".format(100.0 * (b["w"] / d - b["be_sum"] / b["be_n"]))
            if d and b["be_n"] else "   -")
    rec = str(b["w"]) + "-" + str(b["l"]) + "-" + str(b["p"])
    return (str(b["n"]).rjust(4) + "  " + rec.ljust(10)
            + " WR " + wr.rjust(4) + "  BE " + be.rjust(4)
            + "  edge " + edge.rjust(7) + "  units " + format(b["u"], "+.2f").rjust(8))


def _line_band(us_value):
    if us_value is None:
        return "no line"
    if us_value <= 7.5:
        return "<= 7.5 (low)"
    if us_value <= 8.5:
        return "8.0-8.5"
    if us_value <= 9.5:
        return "9.0-9.5"
    return ">= 10 (high)"


LINE_BANDS = ["<= 7.5 (low)", "8.0-8.5", "9.0-9.5", ">= 10 (high)", "no line"]


def _hold_band(h):
    if h is None:
        return "no hold"
    if h < 0:
        return "negative (arb-ish)"
    if h < 0.02:
        return "0-2% (tight)"
    if h < 0.03:
        return "2-3% (edge)"
    if h < 0.04:
        return "3-4%"
    return ">= 4% (wide)"


HOLD_BANDS = ["negative (arb-ish)", "0-2% (tight)", "2-3% (edge)", "3-4%", ">= 4% (wide)", "no hold"]


def _price_band(price):
    if price is None:
        return "no price"
    if price >= -110:
        return "<= -110 / +odds"
    if price >= -115:
        return "-111..-115"
    if price >= -125:
        return "-116..-125"
    if price >= -140:
        return "-126..-140"
    return "worse than -140"


PRICE_BANDS = ["<= -110 / +odds", "-111..-115", "-116..-125", "-126..-140", "worse than -140", "no price"]


def _disp_band(disp):
    if disp is None:
        return "no disp"
    if disp == 0:
        return "0 (books agree)"
    if disp <= 0.5:
        return "0-0.5"
    return "> 0.5"


DISP_BANDS = ["0 (books agree)", "0-0.5", "> 0.5", "no disp"]


def _print_split(title, order, table, read):
    print("\n=== " + title + " ===")
    print("  bucket                 n   W-L-P      WR    BE    edge      units")
    for key in order:
        b = table.get(key)
        if b and b["n"]:
            print("  " + key.ljust(20) + " " + _fmt(b))
    print("  " + read)


def main():
    since = _since()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    sport_clause = "" if SPORT == "all" else " AND sport_key = ?"
    params = [since] + ([] if SPORT == "all" else [SPORT])
    sql = """
        SELECT result, outcome_name, signal_strength, details_json
        FROM (
            SELECT result, outcome_name, signal_strength, details_json,
                   event_id, ROW_NUMBER() OVER (
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

    overall = _new()
    strength_dist = {}   # rounded strength -> count
    delta_dist = {}      # rounded delta -> count
    by_side = {}
    by_line = {b: _new() for b in LINE_BANDS}
    by_hold = {b: _new() for b in HOLD_BANDS}
    by_price = {b: _new() for b in PRICE_BANDS}
    by_disp = {b: _new() for b in DISP_BANDS}

    for row in cur:
        d = _details(row["details_json"])
        result = row["result"]
        price = _price(d)
        _add(overall, result, price)

        st = row["signal_strength"]
        skey = ("{:.2f}".format(st)) if st is not None else "none"
        strength_dist[skey] = strength_dist.get(skey, 0) + 1

        delta = d.get("delta")
        if delta is None:
            us, pin = d.get("us_value"), d.get("pinnacle_value")
            if us is not None and pin is not None:
                delta = round(abs(us - pin), 2)
        dkey = ("{:.2f}".format(delta)) if delta is not None else "none"
        delta_dist[dkey] = delta_dist.get(dkey, 0) + 1

        side = (row["outcome_name"] or "?").title()
        by_side.setdefault(side, _new())
        _add(by_side[side], result, price)

        _add(by_line[_line_band(d.get("us_value"))], result, price)
        _add(by_hold[_hold_band(d.get("cross_book_hold"))], result, price)
        _add(by_price[_price_band(price)], result, price)
        _add(by_disp[_disp_band(d.get("dispersion"))], result, price)

    conn.close()

    scope = SPORT if SPORT != "all" else "all sports"
    print("MLB PD-totals strength meaning (" + scope + ") - DB: " + DB_PATH)
    print("Since " + since[:10] + " (by resolved_at); sent-only; dedup latest fire")
    print("\nOVERALL: " + _fmt(overall))

    print("\n=== PART 1: PROOF strength is a dead constant ===")
    print("  reported strength value -> count")
    for k in sorted(strength_dist):
        print("    " + k.rjust(5) + " : " + str(strength_dist[k]))
    print("  underlying delta (runs) -> count")
    for k in sorted(delta_dist):
        print("    " + k.rjust(5) + " : " + str(delta_dist[k]))
    print("  Read: one (or near-one) strength value => the metric ranks nothing.")

    print("\n=== PART 2: dimensions that DO vary -- candidates for a real strength ===")
    print("(edge = WR - break-even WR; the bar a price must clear to profit)")
    _print_split("BY SIDE (Over vs Under)", sorted(by_side), by_side,
                 "Read: a persistent Over/Under skew is free directional info.")
    _print_split("BY TOTAL LINE (low- vs high-scoring games)", LINE_BANDS, by_line,
                 "Read: does the edge concentrate in pitcher's-duel or slugfest totals?")
    _print_split("BY CROSS-BOOK HOLD (market sharpness)", HOLD_BANDS, by_hold,
                 "Read: 0-2% is mostly filtered at fire; compare negative vs 2%+.")
    _print_split("BY VALUE-BOOK PRICE (the juice we take)", PRICE_BANDS, by_price,
                 "Read: confirms the -116..-125 sweet spot or shows juice eats edge.")
    _print_split("BY US-BOOK DISPERSION (outlier strength)", DISP_BANDS, by_disp,
                 "Read: is a lone outlier book sharper than a cluster that agrees?")

    print("\nNEXT: whichever dimension shows the widest, sample-backed edge spread")
    print("is the basis for an MLB-specific strength (e.g. blend price band +")
    print("hold + side) to replace the constant 0.33 that means nothing today.")


if __name__ == "__main__":
    main()
