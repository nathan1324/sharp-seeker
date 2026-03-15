"""Analyze Pinnacle Divergence win rate by narrow strength bands.

Usage:
    docker compose exec sharp-seeker python /app/scripts/analyze_pd_strength.py
    docker compose exec sharp-seeker python /app/scripts/analyze_pd_strength.py 2026-03-05
"""

import sqlite3
import sys
import time
from collections import defaultdict

DB = "/app/data/sharp_seeker.db"


def connect():
    for attempt in range(10):
        try:
            conn = sqlite3.connect(DB, timeout=10)
            conn.row_factory = sqlite3.Row
            conn.execute("SELECT 1 FROM signal_results LIMIT 1")
            return conn
        except sqlite3.OperationalError:
            print("  DB locked, retrying ({n}/10)...".format(n=attempt + 1))
            time.sleep(3)
    raise SystemExit("ERROR: Could not acquire DB lock after 10 attempts.")


def main():
    since = sys.argv[1] if len(sys.argv) > 1 else None
    conn = connect()

    if since:
        rows = conn.execute("""
            SELECT signal_strength, result, sport_key, market_key
            FROM signal_results
            WHERE signal_type = 'pinnacle_divergence'
              AND result IS NOT NULL
              AND signal_at >= ?
            ORDER BY signal_at
        """, (since,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT signal_strength, result, sport_key, market_key
            FROM signal_results
            WHERE signal_type = 'pinnacle_divergence'
              AND result IS NOT NULL
            ORDER BY signal_at
        """).fetchall()

    conn.close()

    if not rows:
        print("No resolved PD signals found.")
        return

    since_label = " (since {d})".format(d=since) if since else ""
    print("=== PINNACLE DIVERGENCE BY STRENGTH{label} ===".format(label=since_label))
    print("Total PD signals: {n}".format(n=len(rows)))
    print()

    # 5% bands from 30% to 85%
    bands = []
    for lo in range(30, 85, 5):
        bands.append((lo / 100, (lo + 5) / 100, "{lo}-{hi}%".format(lo=lo, hi=lo + 5)))

    print("{band:<12s} {w:>5s} {l:>5s} {n:>5s} {pct:>7s}".format(
        band="Strength", w="W", l="L", n="Total", pct="Win%",
    ))
    print("-" * 40)

    for lo, hi, label in bands:
        band_rows = [r for r in rows if lo <= dict(r)["signal_strength"] < hi]
        if not band_rows:
            continue
        w = sum(1 for r in band_rows if dict(r)["result"] == "won")
        l = sum(1 for r in band_rows if dict(r)["result"] == "lost")
        decided = w + l
        pct = "{p:.0f}%".format(p=w / decided * 100) if decided else "N/A"
        marker = " ***" if decided >= 10 and w / decided >= 0.58 else ""
        marker = " !!!" if decided >= 10 and w / decided < 0.50 else marker
        print("{label:<12s} {w:>5d} {l:>5d} {n:>5d} {pct:>7s}{marker}".format(
            label=label, w=w, l=l, n=decided, pct=pct, marker=marker,
        ))

    # Cumulative: "at or above X%" to find the cutoff
    print()
    print("=== CUMULATIVE: AT OR ABOVE THRESHOLD ===")
    print("{thresh:<15s} {w:>5s} {l:>5s} {n:>5s} {pct:>7s}".format(
        thresh="Threshold", w="W", l="L", n="Total", pct="Win%",
    ))
    print("-" * 42)

    for thresh in range(30, 80, 5):
        t = thresh / 100
        above = [r for r in rows if dict(r)["signal_strength"] >= t]
        w = sum(1 for r in above if dict(r)["result"] == "won")
        l = sum(1 for r in above if dict(r)["result"] == "lost")
        decided = w + l
        pct = "{p:.0f}%".format(p=w / decided * 100) if decided else "N/A"
        print(">= {t}%{pad} {w:>5d} {l:>5d} {n:>5d} {pct:>7s}".format(
            t=thresh, pad=" " * (10 - len(str(thresh))), w=w, l=l, n=decided, pct=pct,
        ))

    # Also break down by sport within each band
    print()
    print("=== BY STRENGTH x SPORT ===")
    sports = sorted(set(dict(r).get("sport_key", "unknown") for r in rows))
    sport_short = {
        "basketball_nba": "NBA",
        "basketball_ncaab": "NCAAB",
        "icehockey_nhl": "NHL",
    }

    for lo, hi, label in bands:
        band_rows = [r for r in rows if lo <= dict(r)["signal_strength"] < hi]
        if not band_rows:
            continue
        print("\n  {label}:".format(label=label))
        for sp in sports:
            sp_rows = [r for r in band_rows if dict(r).get("sport_key") == sp]
            if not sp_rows:
                continue
            w = sum(1 for r in sp_rows if dict(r)["result"] == "won")
            l = sum(1 for r in sp_rows if dict(r)["result"] == "lost")
            decided = w + l
            pct = "{p:.0f}%".format(p=w / decided * 100) if decided else "N/A"
            sp_label = sport_short.get(sp, sp)
            print("    {sp:<10s} {w:>3d}-{l:<3d} ({pct})  n={n}".format(
                sp=sp_label, w=w, l=l, pct=pct, n=decided,
            ))


if __name__ == "__main__":
    main()
