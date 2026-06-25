"""How much juice would best-price shopping have recovered? (read-only)

The best-price/line-shopping fix (market-side dedup) makes us take the best
available price among PD-active US books AT THE SIGNAL'S NUMBER, instead of an
arbitrary book. This measures, retrospectively, how much that would have saved on
already-sent signals — so we know what deploying it actually buys before waiting
a week for live data.

Method: for each sent+graded PD totals signal, look up the exact snapshot the
detector saw (signal_results.signal_at == odds_snapshots.fetched_at) and find the
best price among DraftKings/FanDuel/BetRivers (the PD-active books) on the SAME
side at the SAME point. Compare to the recorded price.

Recovery is computed in the recap's flat-to-win-1u convention, where a win pays
+1 regardless of price, so a better price only shrinks the stake-at-risk on a
LOSS. Recoverable units = sum over losing bets of (risk_recorded - risk_best).
This is exactly the amount recorded units would have improved.

Same-point only (different points are a different bet — that's point-buying, a
separate analysis). Sent-only, windowed by resolved_at, dedup latest fire.
Read-only; streams (server has ~954MB RAM).

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/measure_recoverable_juice.py [days|since-date] [db_path] [sport]
  (defaults: 30 days, db=/app/data/sharp_seeker.db, sport=baseball_mlb; pass "all" for every sport)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

ARG1 = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else "30"
DB_PATH = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "/app/data/sharp_seeker.db"
SPORT = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else "baseball_mlb"

# PD-active US books — betmgm + williamhill_us are excluded from PD, so the merge
# only ever shops among these three.
PD_BOOKS = ("draftkings", "fanduel", "betrivers")


def _since():
    if "-" in ARG1:
        return ARG1 if "T" in ARG1 else ARG1 + "T00:00:00+00:00"
    return (datetime.now(timezone.utc) - timedelta(days=int(ARG1))).isoformat()


def _recorded(details_json):
    """(price, point) of the book we actually posted."""
    if not details_json:
        return None, None
    try:
        d = json.loads(details_json) if isinstance(details_json, str) else details_json
        vb = d.get("value_books", [])
        if vb:
            return vb[0].get("price"), vb[0].get("point")
    except (json.JSONDecodeError, TypeError, AttributeError):
        pass
    return None, None


def _risk(price):
    return abs(price) / 100.0 if price < 0 else 100.0 / price


def main():
    since = _since()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    sport_clause = "" if SPORT == "all" else " AND sport_key = ?"
    params = [since] + ([] if SPORT == "all" else [SPORT])

    sql = """
        SELECT event_id, outcome_name, signal_at, result, details_json
        FROM (
            SELECT event_id, outcome_name, signal_at, result, details_json,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id, outcome_name
                       ORDER BY signal_at DESC
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
    rows = conn.execute(sql, params).fetchall()

    book_ph = ",".join("?" for _ in PD_BOOKS)
    snap_sql = (
        "SELECT bookmaker_key, price, point FROM odds_snapshots"
        " WHERE event_id = ? AND fetched_at = ? AND market_key = 'totals'"
        " AND outcome_name = ? AND bookmaker_key IN (" + book_ph + ")"
    )

    n = matched = improved = 0
    unmatched = no_price = 0
    cents_better = []          # improvement in cents per improved bet
    rec_units = 0.0            # recoverable units (loss-side)
    old_units = 0.0
    new_units = 0.0
    losses = improved_losses = 0

    for r in rows:
        n += 1
        rec_price, rec_point = _recorded(r["details_json"])
        if rec_price is None or rec_point is None:
            no_price += 1
            continue

        snaps = conn.execute(
            snap_sql, (r["event_id"], r["signal_at"], r["outcome_name"], *PD_BOOKS)
        ).fetchall()
        # best price among PD books at the SAME point
        prices = [
            s["price"] for s in snaps
            if s["point"] is not None and abs(s["point"] - rec_point) < 0.01
            and s["price"] is not None
        ]
        if not prices:
            unmatched += 1
            continue
        matched += 1
        best = max(prices)                       # higher American = better for bettor
        best = max(best, rec_price)              # never worse than what we posted

        result = r["result"]
        # old (recorded) units, flat-to-win-1u
        if result == "won":
            old_units += 1.0
            new_units += 1.0
        elif result == "lost":
            losses += 1
            old_units -= _risk(rec_price)
            new_units -= _risk(best)
            if best > rec_price:
                rec_units += _risk(rec_price) - _risk(best)
                improved_losses += 1
        # push: 0

        if best > rec_price:
            improved += 1
            cents_better.append(abs(best - rec_price))

    conn.close()

    scope = SPORT if SPORT != "all" else "all sports"
    print("Recoverable-juice measurement (PD totals, " + scope + ") - DB: " + DB_PATH)
    print("Since " + since[:10] + " (by resolved_at); sent-only; books "
          + "/".join(PD_BOOKS) + "; same-point only\n")

    print("Signals (deduped):        " + str(n))
    print("  no recorded price:      " + str(no_price))
    print("  no matching snapshot:   " + str(unmatched))
    print("  matched to snapshots:   " + str(matched))
    if matched:
        print("\nA better price WAS available at the same number: " + str(improved)
              + "  (" + format(100.0 * improved / matched, ".0f") + "% of matched)")
    if cents_better:
        cents_better.sort()
        avg_c = sum(cents_better) / len(cents_better)
        med_c = cents_better[len(cents_better) // 2]
        print("  avg improvement: " + format(avg_c, ".0f") + " cents   median: "
              + format(med_c, ".0f") + " cents   max: " + str(int(cents_better[-1])))

    print("\n=== RECOVERABLE UNITS (flat-to-win-1u, loss-side) ===")
    print("Losses: " + str(losses) + "   of which a better price existed: " + str(improved_losses))
    print("Recorded units (these matched bets):   " + format(old_units, "+.2f"))
    print("If best-priced at the same number:      " + format(new_units, "+.2f"))
    print("=> Recoverable: " + format(new_units - old_units, "+.2f") + "u")
    print("\nThis is what best-price shopping (already merged) would have saved on")
    print("these bets. Different-NUMBER moves at a better price (point-buying) are")
    print("a separate, larger lever measured elsewhere.")


if __name__ == "__main__":
    main()
