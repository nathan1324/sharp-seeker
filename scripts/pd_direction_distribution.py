"""Diagnose the PD sharp-line direction annotation on fired h2h/spread signals.

READ-ONLY. The directional tracking added 2026-06-01 tags each PD h2h/spread
signal with how Pinnacle's line moved over the recent window:
  toward  = sharp line moved to back the flagged side (our value confirmed)
  against = sharp line moved to fade it (our "value" may be a falling knife)
  flat    = negligible move
  unknown = < 2 Pinnacle snapshots in window (can't tell)

This script answers two things:
  1. Is the measurement WORKING, or is it mostly unknown/flat (i.e. the 30-min
     window + 12-min poll cadence is too sparse to ever see a move)?
  2. Where data is graded, do "against" signals underperform "toward"?

It also reports the earliest annotated signal (when the feature went live) and
how many fired signals predate it (no annotation = MISSING).

Usage (server):
    docker compose exec sharp-seeker python /app/scripts/pd_direction_distribution.py
Optional args: [db_path] [since_iso]
"""

import json
import sqlite3
import sys
from collections import defaultdict

DB_PATH = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else "/app/data/sharp_seeker.db"
SINCE = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else None

DIRECTIONS = ["toward", "against", "flat", "unknown", "MISSING"]


def _wr(won, lost):
    decided = won + lost
    return (100.0 * won / decided) if decided else 0.0


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    sql = (
        "SELECT sport_key, market_key, outcome_name, signal_strength, signal_at, "
        "result, details_json FROM signal_results "
        "WHERE signal_type = 'pinnacle_divergence' AND market_key IN ('h2h','spreads')"
    )
    params = []
    if SINCE:
        sql += " AND signal_at >= ?"
        params.append(SINCE)
    sql += " ORDER BY signal_at"

    total = 0
    annotated = 0
    earliest_annotated = None
    # counts[(market, direction)] -> total count
    counts = defaultdict(int)
    # graded[(market, direction)] -> [won, lost, push]
    graded = defaultdict(lambda: [0, 0, 0])
    # by_sport[(sport, direction)] -> count
    by_sport = defaultdict(int)
    flat_deltas = []  # |delta| for flat h2h, to sanity-check the 0.5% band

    for r in db.execute(sql, params):
        total += 1
        market = r["market_key"]
        try:
            details = json.loads(r["details_json"]) if r["details_json"] else {}
        except (ValueError, TypeError):
            details = {}
        direction = details.get("pinnacle_recent_direction")
        if direction is None:
            direction = "MISSING"
        else:
            annotated += 1
            if earliest_annotated is None or r["signal_at"] < earliest_annotated:
                earliest_annotated = r["signal_at"]
            if direction == "flat" and market == "h2h":
                d = details.get("pinnacle_recent_delta")
                if isinstance(d, (int, float)):
                    flat_deltas.append(abs(d))

        counts[(market, direction)] += 1
        by_sport[(r["sport_key"] or "?", direction)] += 1
        res = (r["result"] or "").lower()
        if res in ("won", "win"):
            graded[(market, direction)][0] += 1
        elif res in ("lost", "loss"):
            graded[(market, direction)][1] += 1
        elif res in ("push", "tie"):
            graded[(market, direction)][2] += 1
    db.close()

    span = f" (since {SINCE})" if SINCE else " (all stored data)"
    print(f"PD sharp-line direction distribution{span} - DB: {DB_PATH}")
    print(f"Total PD h2h/spread signal_results rows: {total}")
    print(f"  with direction annotation: {annotated}    pre-feature (MISSING): {total - annotated}")
    print(f"  earliest annotated signal_at: {earliest_annotated}\n")

    if annotated == 0:
        print("No annotated signals yet — feature just went live or no PD h2h/spread")
        print("signals have fired since deploy. Re-run after a polling cycle or two.")
        return

    for market in ("h2h", "spreads"):
        mtotal = sum(counts[(market, d)] for d in DIRECTIONS)
        if mtotal == 0:
            continue
        print(f"  {market.upper()}  (n={mtotal})")
        print("    {:<9} {:>6} {:>7}   {:>10}  {}".format(
            "dir", "count", "% ", "W-L-P", "win%"))
        print("    " + "-" * 48)
        for d in DIRECTIONS:
            n = counts[(market, d)]
            if n == 0:
                continue
            pct = 100.0 * n / mtotal
            w, l, p = graded[(market, d)]
            wlp = f"{w}-{l}-{p}"
            wr = _wr(w, l)
            wr_str = f"{wr:.0f}%" if (w + l) else "-"
            print("    {:<9} {:>6} {:>6.1f}%   {:>10}  {}".format(d, n, pct, wlp, wr_str))
        print()

    # Quick read on whether the flat band is swallowing real moves
    if flat_deltas:
        avg = sum(flat_deltas) / len(flat_deltas)
        mx = max(flat_deltas)
        print(f"  h2h 'flat' sanity: {len(flat_deltas)} flats, |delta| avg={avg:.4f} "
              f"max={mx:.4f} (band is <0.005)")

    # By-sport direction mix (annotated only), to spot sparsity per sport
    print("\n  Direction mix by sport (annotated signals):")
    sports = sorted({s for (s, _d) in by_sport})
    print("    {:<18} {:>7} {:>8} {:>6} {:>8}".format(
        "sport", "toward", "against", "flat", "unknown"))
    for s in sports:
        row = [by_sport[(s, d)] for d in ("toward", "against", "flat", "unknown")]
        if sum(row) == 0:
            continue
        print("    {:<18} {:>7} {:>8} {:>6} {:>8}".format(s, *row))


if __name__ == "__main__":
    main()
