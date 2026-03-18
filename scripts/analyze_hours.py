"""Comprehensive hourly performance analysis for all signal types.

Outputs:
  1. Win rate by MST hour for each signal type (all-time)
  2. Comparison: current best hours vs data-recommended
  3. Comparison: current quiet hours vs data-recommended suppressions
  4. Suggested config changes

Usage:
    docker compose exec sharp-seeker python /app/scripts/analyze_hours.py
"""

import sqlite3
import time
from collections import defaultdict

DB = "/app/data/sharp_seeker.db"

# Current config for comparison
CURRENT_BEST_HOURS = {
    "pinnacle_divergence": {5, 12, 14, 16, 17},
    "reverse_line": {8, 12},
    "rapid_change": {18},
    "steam_move": {11, 13},
}
CURRENT_QUIET_HOURS_UTC = {
    "pinnacle_divergence": {3, 14, 22},
    "steam_move": {0, 1, 2, 3, 4, 15},
    "rapid_change": {1, 3, 21},
}

SIGNAL_SHORT = {
    "pinnacle_divergence": "PD",
    "steam_move": "SM",
    "rapid_change": "RC",
    "reverse_line": "RL",
    "exchange_shift": "ES",
}


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


def _wr(w, l):
    d = w + l
    if d == 0:
        return " N/A"
    return "{p:5.1f}%".format(p=w / d * 100)


def _utc_to_mst(utc_hour):
    return (utc_hour - 7) % 24


def _mst_to_utc(mst_hour):
    return (mst_hour + 7) % 24


def main():
    conn = connect()

    signals = conn.execute("""
        SELECT signal_type, sport_key, market_key, outcome_name,
               signal_at, signal_strength, result
        FROM signal_results
        WHERE result IS NOT NULL
        ORDER BY signal_at
    """).fetchall()
    signals = [dict(r) for r in signals]

    if not signals:
        print("No resolved signals found.")
        return

    # Parse hours
    for s in signals:
        try:
            utc_h = int(s["signal_at"][11:13])
            s["utc_hour"] = utc_h
            s["mst_hour"] = _utc_to_mst(utc_h)
        except (ValueError, IndexError):
            s["utc_hour"] = -1
            s["mst_hour"] = -1

    sig_types = sorted(set(s["signal_type"] for s in signals))

    print("=" * 75)
    print("  HOURLY PERFORMANCE ANALYSIS")
    print("  Total resolved signals: {n}".format(n=len(signals)))
    print("=" * 75)

    # ═══════════════════════════════════════════════════════════════
    # 1. PER SIGNAL TYPE: WIN RATE BY MST HOUR
    # ═══════════════════════════════════════════════════════════════
    type_hour_data = {}  # {signal_type: {mst_hour: {won, lost}}}

    for st in sig_types:
        short = SIGNAL_SHORT.get(st, st)
        st_sigs = [s for s in signals if s["signal_type"] == st]
        total_w = sum(1 for s in st_sigs if s["result"] == "won")
        total_l = sum(1 for s in st_sigs if s["result"] == "lost")

        print()
        print("=" * 75)
        print("  {short} ({st}) — {w}-{l} ({wr}) overall".format(
            short=short, st=st, w=total_w, l=total_l, wr=_wr(total_w, total_l),
        ))
        print("=" * 75)
        print("  {mst:>7s}  {utc:>7s}  {w:>4s} {l:>4s} {n:>5s} {wr:>7s}  {bar}".format(
            mst="MST", utc="UTC", w="W", l="L", n="Dec", wr="Win%", bar="",
        ))
        print("  " + "-" * 70)

        best_hours = CURRENT_BEST_HOURS.get(st, set())
        quiet_utc = CURRENT_QUIET_HOURS_UTC.get(st, set())
        hour_data = {}

        for mst_h in range(24):
            utc_h = _mst_to_utc(mst_h)
            h_sigs = [s for s in st_sigs if s["mst_hour"] == mst_h]
            w = sum(1 for s in h_sigs if s["result"] == "won")
            l = sum(1 for s in h_sigs if s["result"] == "lost")
            d = w + l
            hour_data[mst_h] = {"won": w, "lost": l, "decided": d}

            if d == 0:
                continue

            rate = w / d * 100 if d > 0 else 0
            bar_len = int(rate / 5)
            bar = "#" * bar_len

            markers = []
            if mst_h in best_hours:
                markers.append("BEST")
            if utc_h in quiet_utc:
                markers.append("QUIET")
            # Flag hours
            if d >= 10 and rate >= 58:
                markers.append("***")
            elif d >= 10 and rate < 45:
                markers.append("!!!")
            elif d >= 5 and rate < 40:
                markers.append("!!")

            marker_str = "  [{m}]".format(m=", ".join(markers)) if markers else ""

            print("  {mst:>4d}:00  {utc:>4d}:00  {w:>4d} {l:>4d} {n:>5d} {wr}  {bar}{m}".format(
                mst=mst_h, utc=utc_h, w=w, l=l, n=d,
                wr=_wr(w, l), bar=bar, m=marker_str,
            ))

        type_hour_data[st] = hour_data

    # ═══════════════════════════════════════════════════════════════
    # 2. BEST HOURS AUDIT — are current best hours still earning it?
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 75)
    print("  BEST HOURS AUDIT")
    print("  Current config vs actual performance")
    print("=" * 75)

    for st in sig_types:
        short = SIGNAL_SHORT.get(st, st)
        best_hours = CURRENT_BEST_HOURS.get(st, set())
        hour_data = type_hour_data.get(st, {})
        st_sigs = [s for s in signals if s["signal_type"] == st]
        total_w = sum(1 for s in st_sigs if s["result"] == "won")
        total_l = sum(1 for s in st_sigs if s["result"] == "lost")
        total_d = total_w + total_l
        avg_rate = total_w / total_d * 100 if total_d else 0

        if not best_hours and not hour_data:
            continue

        print()
        print("  {short}:".format(short=short))
        print("    Overall avg: {r:.1f}%".format(r=avg_rate))

        if best_hours:
            print("    Current best hours (MST): {h}".format(
                h=sorted(best_hours),
            ))
            for mst_h in sorted(best_hours):
                hd = hour_data.get(mst_h, {"won": 0, "lost": 0, "decided": 0})
                rate = hd["won"] / hd["decided"] * 100 if hd["decided"] else 0
                diff = rate - avg_rate
                status = "OK" if (hd["decided"] >= 5 and rate > avg_rate) else "REVIEW"
                if hd["decided"] < 5:
                    status = "LOW-N"
                print("      MST {h:02d}: {w}-{l} ({wr})  n={n}  delta={d:+.0f}pp  [{s}]".format(
                    h=mst_h, w=hd["won"], l=hd["lost"],
                    wr=_wr(hd["won"], hd["lost"]),
                    n=hd["decided"], d=diff, s=status,
                ))

        # Find hours that SHOULD be best hours (>= avg + 5pp, n >= 10)
        candidates = []
        for mst_h in range(24):
            hd = hour_data.get(mst_h, {"won": 0, "lost": 0, "decided": 0})
            if hd["decided"] < 10:
                continue
            rate = hd["won"] / hd["decided"] * 100
            if rate >= avg_rate + 5 and mst_h not in best_hours:
                candidates.append((mst_h, hd["won"], hd["lost"], hd["decided"], rate))

        if candidates:
            print("    Candidates to ADD (>= avg+5pp, n>=10):")
            for mst_h, w, l, n, rate in sorted(candidates, key=lambda x: -x[4]):
                print("      MST {h:02d}: {w}-{l} ({wr})  n={n}  delta={d:+.0f}pp".format(
                    h=mst_h, w=w, l=l, wr=_wr(w, l), n=n, d=rate - avg_rate,
                ))

    # ═══════════════════════════════════════════════════════════════
    # 3. QUIET HOURS AUDIT — are we suppressing the right hours?
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 75)
    print("  QUIET HOURS AUDIT")
    print("  Current suppressed UTC hours vs actual performance")
    print("=" * 75)

    for st in sig_types:
        short = SIGNAL_SHORT.get(st, st)
        quiet_utc = CURRENT_QUIET_HOURS_UTC.get(st, set())
        hour_data = type_hour_data.get(st, {})
        st_sigs = [s for s in signals if s["signal_type"] == st]
        total_w = sum(1 for s in st_sigs if s["result"] == "won")
        total_l = sum(1 for s in st_sigs if s["result"] == "lost")
        total_d = total_w + total_l
        avg_rate = total_w / total_d * 100 if total_d else 0

        print()
        print("  {short}:".format(short=short))

        if quiet_utc:
            print("    Current quiet hours (UTC): {h}".format(h=sorted(quiet_utc)))
            for utc_h in sorted(quiet_utc):
                mst_h = _utc_to_mst(utc_h)
                hd = hour_data.get(mst_h, {"won": 0, "lost": 0, "decided": 0})
                rate = hd["won"] / hd["decided"] * 100 if hd["decided"] else 0
                diff = rate - avg_rate
                status = "OK" if (hd["decided"] >= 5 and rate < avg_rate) else "REVIEW"
                if hd["decided"] < 5:
                    status = "LOW-N"
                print("      UTC {u:02d} (MST {m:02d}): {w}-{l} ({wr})  n={n}  delta={d:+.0f}pp  [{s}]".format(
                    u=utc_h, m=mst_h, w=hd["won"], l=hd["lost"],
                    wr=_wr(hd["won"], hd["lost"]),
                    n=hd["decided"], d=diff, s=status,
                ))
        else:
            print("    No quiet hours configured.")

        # Find hours that SHOULD be quiet (< avg - 5pp, n >= 10)
        candidates = []
        for mst_h in range(24):
            utc_h = _mst_to_utc(mst_h)
            hd = hour_data.get(mst_h, {"won": 0, "lost": 0, "decided": 0})
            if hd["decided"] < 10:
                continue
            rate = hd["won"] / hd["decided"] * 100
            if rate < avg_rate - 5 and utc_h not in quiet_utc:
                candidates.append((utc_h, mst_h, hd["won"], hd["lost"], hd["decided"], rate))

        if candidates:
            print("    Candidates to SUPPRESS (< avg-5pp, n>=10):")
            for utc_h, mst_h, w, l, n, rate in sorted(candidates, key=lambda x: x[5]):
                print("      UTC {u:02d} (MST {m:02d}): {w}-{l} ({wr})  n={n}  delta={d:+.0f}pp".format(
                    u=utc_h, m=mst_h, w=w, l=l, wr=_wr(w, l), n=n, d=rate - avg_rate,
                ))

    # ═══════════════════════════════════════════════════════════════
    # 4. COMBINED: BEST vs WORST HOURS PERFORMANCE
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 75)
    print("  BEST HOURS vs NON-BEST HOURS PERFORMANCE")
    print("=" * 75)

    for st in sig_types:
        short = SIGNAL_SHORT.get(st, st)
        best_hours = CURRENT_BEST_HOURS.get(st, set())
        if not best_hours:
            continue

        st_sigs = [s for s in signals if s["signal_type"] == st]
        in_best = [s for s in st_sigs if s["mst_hour"] in best_hours]
        not_best = [s for s in st_sigs if s["mst_hour"] not in best_hours]

        bw = sum(1 for s in in_best if s["result"] == "won")
        bl = sum(1 for s in in_best if s["result"] == "lost")
        nw = sum(1 for s in not_best if s["result"] == "won")
        nl = sum(1 for s in not_best if s["result"] == "lost")

        print("  {short}:  Best hours {bw}-{bl} ({bwr})  |  Other hours {nw}-{nl} ({nwr})".format(
            short=short, bw=bw, bl=bl, bwr=_wr(bw, bl),
            nw=nw, nl=nl, nwr=_wr(nw, nl),
        ))

    conn.close()

    print()
    print("=" * 75)
    print("  ANALYSIS COMPLETE")
    print("=" * 75)


if __name__ == "__main__":
    main()
