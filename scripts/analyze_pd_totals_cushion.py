"""Does point-buying help PD totals? The edge-vs-cushion tradeoff (read-only).

A point-buy toward a BETTER PRICE moves our line toward Pinnacle's fair number,
i.e. it sells the cushion (the gap between our line `us_value` and Pinnacle's
`pinnacle_value`) that is the whole PD edge. A point-buy toward MORE cushion
costs more juice. So point-buying can only ever trade edge against vig — it can't
deliver both. This quantifies that tradeoff from stored signals:

  - WR and units by cushion bucket -> is the edge actually IN the cushion?
  - avg price by cushion bucket    -> does more cushion cost more juice?

If WR rises with cushion (and price worsens with it), buying a better price by
shrinking the cushion is selling the edge — point-buying is not the lever. If WR
is flat across cushion, the cushion isn't earning its juice and trimming price
(or the line) is worth a closer look.

A retrospective EV of an actual point-buy is NOT computable here: we don't store
final game totals or historical alt prices, so we can't re-grade a moved line.
This measures the tradeoff, not a counterfactual fill.

Sent-only, windowed by resolved_at, dedup latest fire. Read-only; streams.

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_pd_totals_cushion.py [days|since-date] [db_path] [sport]
  (defaults: 30 days, db=/app/data/sharp_seeker.db, sport=baseball_mlb; "all" for every sport)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

ARG1 = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else "30"
DB_PATH = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "/app/data/sharp_seeker.db"
SPORT = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else "baseball_mlb"

# Cushion buckets (runs between our line and Pinnacle's fair number).
BUCKETS = [(1.0, 1.25), (1.25, 1.5), (1.5, 2.0), (2.0, 99.0)]


def _since():
    if "-" in ARG1:
        return ARG1 if "T" in ARG1 else ARG1 + "T00:00:00+00:00"
    return (datetime.now(timezone.utc) - timedelta(days=int(ARG1))).isoformat()


def _parse(details_json):
    """(cushion, price) from a signal's details."""
    if not details_json:
        return None, None
    try:
        d = json.loads(details_json) if isinstance(details_json, str) else details_json
    except (json.JSONDecodeError, TypeError):
        return None, None
    cushion = d.get("delta")
    if cushion is None:
        us, pin = d.get("us_value"), d.get("pinnacle_value")
        if us is not None and pin is not None:
            cushion = abs(us - pin)
    price = None
    vb = d.get("value_books", [])
    if vb:
        price = vb[0].get("price")
    return cushion, price


def _risk(price):
    return abs(price) / 100.0 if price < 0 else 100.0 / price


def _be(price):
    return abs(price) / (abs(price) + 100.0) if price < 0 else 100.0 / (price + 100.0)


def _new():
    return {"n": 0, "w": 0, "l": 0, "p": 0, "u": 0.0, "be_sum": 0.0, "be_n": 0,
            "price_sum": 0.0, "price_n": 0}


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
    b["price_sum"] += price
    b["price_n"] += 1
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
    ap = ("{:+.0f}".format(b["price_sum"] / b["price_n"])) if b["price_n"] else "  -"
    rec = str(b["w"]) + "-" + str(b["l"]) + "-" + str(b["p"])
    return (str(b["n"]).rjust(4) + "  " + rec.ljust(10)
            + " WR " + wr.rjust(4) + "  BE " + be.rjust(4)
            + "  avg " + ap.rjust(5) + "  units " + format(b["u"], "+.2f").rjust(8))


def _label(lo, hi):
    return (format(lo, ".2f") + "-" + format(hi, ".2f")) if hi < 90 else (format(lo, ".2f") + "+")


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

    overall = _new()
    buckets = {b: _new() for b in BUCKETS}
    no_cushion = 0
    for row in cur:
        cushion, price = _parse(row["details_json"])
        _add(overall, row["result"], price)
        if cushion is None:
            no_cushion += 1
            continue
        for (lo, hi) in BUCKETS:
            if lo <= cushion < hi:
                _add(buckets[(lo, hi)], row["result"], price)
                break
    conn.close()

    scope = SPORT if SPORT != "all" else "all sports"
    print("PD totals cushion/edge tradeoff (" + scope + ") - DB: " + DB_PATH)
    print("Since " + since[:10] + " (by resolved_at); sent-only; dedup latest fire\n")
    print("OVERALL: " + _fmt(overall))
    if no_cushion:
        print("  (" + str(no_cushion) + " rows missing us/pinnacle values - excluded from buckets)")
    print("\nBy cushion (runs our line sits past Pinnacle's fair number):")
    print("  cushion        n   W-L-P      WR    BE    avg price   units")
    for (lo, hi) in BUCKETS:
        b = buckets[(lo, hi)]
        if b["n"]:
            print("  " + _label(lo, hi).ljust(12) + " " + _fmt(b))
    print("\nRead: if WR climbs with cushion while avg price gets MORE negative,")
    print("the edge lives in the cushion and juice is its cost -- point-buying to a")
    print("better price sells the edge. If WR is flat across cushion, the cushion")
    print("isn't paying for its juice and trimming price/line is worth a look.")


if __name__ == "__main__":
    main()
