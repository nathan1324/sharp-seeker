"""How juiced are our sent PD totals, and do the juiced ones underperform? (read-only)

Premise: PD totals are our X free-play workhorse, but we post the value book's
MAIN line at whatever price it carries. If a big share of sends are worse than
-115, the vig is eating the divergence edge and our break-even win rate climbs
out of reach. Before building an "if price worse than -115, check the alt ladder"
feature, this quantifies two things from data we ALREADY have:

  1. FREQUENCY  - how often sent PD totals fire worse than -115 (the alt trigger)
  2. PERFORMANCE - WR / units of the juiced bucket vs the cheap bucket, with the
                   break-even WR each price level demands. If the juiced bucket
                   isn't actually losing ground, the alt feature is low priority.

Counts only signals actually SENT to Discord (a sent_alerts row exists) - the
decision-relevant population, same as the recap/stats/best-hours queries. Pass
"all" as the 3rd arg to include recorded-but-unsent signals too.

Dedups by (event_id, outcome_name) keeping the latest signal_at. Read-only;
streams rows (server has ~954MB RAM).

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_juiced_pd_totals.py [days] [db_path] [all]
  (defaults: days=60, db_path=/app/data/sharp_seeker.db, sent-only)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 60
DB_PATH = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "/app/data/sharp_seeker.db"
SENT_ONLY = not (len(sys.argv) > 3 and sys.argv[3] == "all")

# The alt-lookup trigger: a value-book price strictly more juiced than this fires
# the (proposed) alt-ladder check. -115 -> -116 and beyond trigger; -110/-105/+100 do not.
JUICE_TRIGGER = -115


def _price(details_json):
    """The price the bettor actually gets = value_books[0].price (or None)."""
    if not details_json:
        return None
    try:
        d = json.loads(details_json) if isinstance(details_json, str) else details_json
        vb = d.get("value_books", [])
        if vb:
            return vb[0].get("price")
    except (json.JSONDecodeError, TypeError, AttributeError):
        pass
    return None


def _units(price, result):
    """Flat-to-win-1u (matches the other analyze_* scripts): won = +1.0,
    lost = -risk where risk = stake needed to win 1u, push/no-price = 0."""
    if result == "push" or price is None:
        return 0.0
    risk = abs(price) / 100.0 if price < 0 else 100.0 / price
    return 1.0 if result == "won" else -risk


def _breakeven_wr(price):
    """Win rate needed to break even at this price (American)."""
    if price is None:
        return None
    if price < 0:
        return abs(price) / (abs(price) + 100.0)
    return 100.0 / (price + 100.0)


def _is_juiced(price):
    """Worse (more negative) than the trigger. Positive prices never trigger."""
    return price is not None and price < JUICE_TRIGGER


def _new_bucket():
    return {"n": 0, "w": 0, "l": 0, "p": 0, "u": 0.0, "be_sum": 0.0, "be_n": 0}


def _add(b, result, u, price):
    b["n"] += 1
    b["u"] += u
    if result == "won":
        b["w"] += 1
    elif result == "lost":
        b["l"] += 1
    else:
        b["p"] += 1
    be = _breakeven_wr(price)
    if be is not None:
        b["be_sum"] += be
        b["be_n"] += 1


def _fmt(b):
    decided = b["w"] + b["l"]
    wr = ("{:.0%}".format(b["w"] / decided)) if decided else "  -"
    avg_be = ("{:.0%}".format(b["be_sum"] / b["be_n"])) if b["be_n"] else "  -"
    rec = (str(b["w"]) + "-" + str(b["l"]) + "-" + str(b["p"]))
    return (str(b["n"]).rjust(4) + "   " + rec.ljust(9)
            + "  WR " + wr.rjust(4) + "  (BE " + avg_be.rjust(4) + ")   "
            + format(b["u"], "+.2f").rjust(8) + "u")


# Fine-grained price bands (by the price the bettor takes).
def _band(price):
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


BANDS = ["<= -110 / +odds", "-111..-115", "-116..-125", "-126..-140", "worse than -140", "no price"]


def main():
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=DAYS)).isoformat()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

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
        SELECT sport_key, result, details_json
        FROM (
            SELECT sport_key, result, details_json, event_id, outcome_name,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id, outcome_name
                       ORDER BY signal_at DESC
                   ) AS rn
            FROM signal_results
            WHERE result IS NOT NULL
              AND signal_type = 'pinnacle_divergence'
              AND market_key = 'totals'
              AND signal_at >= ?{sent}
        )
        WHERE rn = 1
    """.format(sent=sent_clause)
    cur = conn.execute(sql, (since,))

    cheap = _new_bucket()    # price NOT worse than -115 (no alt lookup)
    juiced = _new_bucket()   # price worse than -115 (alt lookup would fire)
    by_band = {b: _new_bucket() for b in BANDS}
    juiced_by_sport = {}

    for row in cur:
        price = _price(row["details_json"])
        result = row["result"]
        u = _units(price, result)
        if _is_juiced(price):
            _add(juiced, result, u, price)
            sk = row["sport_key"]
            juiced_by_sport.setdefault(sk, _new_bucket())
            _add(juiced_by_sport[sk], result, u, price)
        else:
            _add(cheap, result, u, price)
        _add(by_band[_band(price)], result, u, price)

    conn.close()

    scope = "SENT to Discord only" if SENT_ONLY else "ALL recorded (incl. unsent)"
    total_n = cheap["n"] + juiced["n"]
    pct = (100.0 * juiced["n"] / total_n) if total_n else 0.0

    print("PD totals juice analysis - DB: " + DB_PATH)
    print("Window: last " + str(DAYS) + " days (since " + since[:10]
          + "); " + scope + "; dedup latest fire per play")
    print("Alt-lookup trigger: value-book price worse than " + str(JUICE_TRIGGER)
          + " (i.e. <= -116)\n")

    print("=== TRIGGER FREQUENCY ===")
    print("Total sent PD totals: " + str(total_n))
    print("Would fire alt lookup (worse than " + str(JUICE_TRIGGER) + "): "
          + str(juiced["n"]) + "  (" + format(pct, ".1f") + "% of sends)\n")

    print("=== PERFORMANCE: cheap vs juiced ===")
    print("  bucket                       n   W-L-P       WR   (BE)       units")
    print("  cheap (>= -115)        " + _fmt(cheap))
    print("  juiced (worse -115)    " + _fmt(juiced))
    print()

    print("=== BY PRICE BAND ===")
    print("  band                         n   W-L-P       WR   (BE)       units")
    for b in BANDS:
        bk = by_band[b]
        if bk["n"]:
            print("  " + b.ljust(20) + " " + _fmt(bk))
    print()

    if juiced_by_sport:
        print("=== JUICED BUCKET BY SPORT (where the alt feature would act) ===")
        print("  sport                        n   W-L-P       WR   (BE)       units")
        for sk in sorted(juiced_by_sport, key=lambda k: juiced_by_sport[k]["n"], reverse=True):
            print("  " + sk.ljust(20) + " " + _fmt(juiced_by_sport[sk]))
        print()

    print("Read: BE = avg break-even WR the prices in that bucket demand. If the")
    print("juiced bucket's WR sits below its BE (or its units are negative while")
    print("cheap is positive), the vig is eating the edge -> alt feature is worth")
    print("building. If juiced units hold up, it's low priority.")


if __name__ == "__main__":
    main()
