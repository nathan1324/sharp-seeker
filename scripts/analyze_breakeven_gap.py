"""Is the thin profit a VIG problem? WR vs break-even WR, and the juice cost in
units, across all sent+graded signals (read-only).

Tests the claim "our record is solid but we're barely positive because the
signals carry so much vig". The decisive number is the gap between our actual
win rate and the break-even WR the prices demand:

  - gap large & positive  -> real edge, vig is NOT the main squeeze (look at
                             sport mix / a few blowout losers instead)
  - gap small (WR ~= BE)  -> vig is eating the edge; best-price shopping,
                             a price ceiling, or point-buying is the lever

Also reports JUICE COST in units = (units if every bet were even-money) minus
(units at the actual price). In the recap's flat-to-win-1u convention a win pays
+1 regardless of price, so juice shows up entirely on losses (a -130 loss costs
1.30u, not 1.00u). Juice cost = how many units the vig took on losing bets.

Sent-only (EXISTS sent_alerts on the same type/market/outcome) and windowed by
resolved_at, matching the daily recap. Arbitrage excluded (guaranteed-profit
instrument, not a directional play). Dedups to the latest fire per play.
Read-only; streams rows (server has ~954MB RAM).

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_breakeven_gap.py [days|since-date] [db_path] [all]
  (defaults: 30 days, db=/app/data/sharp_seeker.db, sent-only; for June MTD pass 2026-06-01)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

ARG1 = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else "30"
DB_PATH = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "/app/data/sharp_seeker.db"
SENT_ONLY = not (len(sys.argv) > 3 and sys.argv[3] == "all")


def _since():
    if "-" in ARG1:  # treat as an ISO date
        return ARG1 if "T" in ARG1 else ARG1 + "T00:00:00+00:00"
    days = int(ARG1)
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def _price(details_json):
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


def _risk(price):
    """Stake needed to win 1u at this American price."""
    return abs(price) / 100.0 if price < 0 else 100.0 / price


def _breakeven_wr(price):
    if price is None:
        return None
    return abs(price) / (abs(price) + 100.0) if price < 0 else 100.0 / (price + 100.0)


def _new():
    return {"n": 0, "w": 0, "l": 0, "p": 0, "u": 0.0, "even_u": 0.0,
            "be_sum": 0.0, "be_n": 0, "price_sum": 0.0, "price_n": 0, "noprice": 0}


def _add(b, result, price):
    b["n"] += 1
    if result == "won":
        b["w"] += 1
    elif result == "lost":
        b["l"] += 1
    else:
        b["p"] += 1
    if price is None:
        b["noprice"] += 1
        return
    b["price_sum"] += price
    b["price_n"] += 1
    be = _breakeven_wr(price)
    if be is not None:
        b["be_sum"] += be
        b["be_n"] += 1
    # flat-to-win-1u (recap convention)
    if result == "won":
        b["u"] += 1.0
        b["even_u"] += 1.0
    elif result == "lost":
        b["u"] -= _risk(price)
        b["even_u"] -= 1.0
    # push: 0 to both


def _wr(b):
    d = b["w"] + b["l"]
    return (b["w"] / d) if d else None


def _avg_be(b):
    return (b["be_sum"] / b["be_n"]) if b["be_n"] else None


def _avg_price(b):
    if not b["price_n"]:
        return None
    return b["price_sum"] / b["price_n"]


def _fmt(b):
    wr = _wr(b)
    be = _avg_be(b)
    wr_s = ("{:.0%}".format(wr)) if wr is not None else "  -"
    be_s = ("{:.0%}".format(be)) if be is not None else "  -"
    gap = ("{:+.0f}".format((wr - be) * 100) + "pp") if (wr is not None and be is not None) else "  -"
    ap = _avg_price(b)
    ap_s = ("{:+.0f}".format(ap)) if ap is not None else "  -"
    rec = str(b["w"]) + "-" + str(b["l"]) + "-" + str(b["p"])
    juice = b["even_u"] - b["u"]
    return (str(b["n"]).rjust(4) + "  " + rec.ljust(10)
            + " WR " + wr_s.rjust(4) + "  BE " + be_s.rjust(4) + "  gap " + gap.rjust(6)
            + "  avg " + ap_s.rjust(5)
            + "  units " + format(b["u"], "+.2f").rjust(8)
            + "  juice " + format(-juice, "+.2f").rjust(7) + "u")


def main():
    since = _since()
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
        SELECT signal_type, sport_key, market_key, result, details_json
        FROM (
            SELECT signal_type, sport_key, market_key, outcome_name, result, details_json,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id, signal_type, market_key, outcome_name
                       ORDER BY signal_at DESC
                   ) AS rn
            FROM signal_results
            WHERE result IS NOT NULL
              AND signal_type != 'arbitrage'
              AND resolved_at >= ?{sent}
        )
        WHERE rn = 1
    """.format(sent=sent_clause)
    cur = conn.execute(sql, (since,))

    overall = _new()
    by_type = {}
    by_sport = {}
    by_combo = {}

    for row in cur:
        price = _price(row["details_json"])
        result = row["result"]
        _add(overall, result, price)
        by_type.setdefault(row["signal_type"], _new())
        _add(by_type[row["signal_type"]], result, price)
        by_sport.setdefault(row["sport_key"] or "?", _new())
        _add(by_sport[row["sport_key"] or "?"], result, price)
        ck = (row["signal_type"] or "?") + ":" + (row["sport_key"] or "?") + ":" + (row["market_key"] or "?")
        by_combo.setdefault(ck, _new())
        _add(by_combo[ck], result, price)

    conn.close()

    scope = "SENT only" if SENT_ONLY else "ALL recorded"
    print("Break-even / vig analysis - DB: " + DB_PATH)
    print("Since " + since[:10] + " (by resolved_at); " + scope
          + "; arb excluded; dedup latest fire\n")
    print("Columns: n  W-L-P  WR  BE(avg)  gap(WR-BE)  avg price  units  juice(units lost to vig)\n")

    print("OVERALL: " + _fmt(overall))
    if overall["noprice"]:
        print("  (" + str(overall["noprice"]) + " graded rows had no recorded price - excluded from BE/units)")
    print()

    print("BY SIGNAL TYPE:")
    for k in sorted(by_type, key=lambda k: by_type[k]["n"], reverse=True):
        print("  " + k.ljust(22) + " " + _fmt(by_type[k]))
    print()

    print("BY SPORT:")
    for k in sorted(by_sport, key=lambda k: by_sport[k]["n"], reverse=True):
        print("  " + k.ljust(22) + " " + _fmt(by_sport[k]))
    print()

    print("BY TYPE:SPORT:MARKET (n >= 5):")
    for k in sorted(by_combo, key=lambda k: by_combo[k]["u"]):
        if by_combo[k]["n"] >= 5:
            print("  " + k.ljust(40) + " " + _fmt(by_combo[k]))
    print()

    juice = overall["even_u"] - overall["u"]
    wr, be = _wr(overall), _avg_be(overall)
    print("=== VERDICT ===")
    if wr is not None and be is not None:
        print("Actual WR " + "{:.1%}".format(wr) + " vs break-even " + "{:.1%}".format(be)
              + "  (edge " + "{:+.1f}".format((wr - be) * 100) + "pp)")
    print("Units: " + format(overall["u"], "+.2f") + "   if every bet were even-money: "
          + format(overall["even_u"], "+.2f") + "   vig cost: " + format(-juice, "+.2f") + "u")
    print("\nIf the edge (WR-BE) is small, vig is the squeeze -> deploy best-price")
    print("shopping, then weigh a price ceiling / point-buying. If the edge is")
    print("healthy but units are thin, the drag is sport mix or a few big losers.")


if __name__ == "__main__":
    main()
