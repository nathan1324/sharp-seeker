"""Analyze signal win rates by type:sport:market combo.

Compares current best combos config against actual performance data.

Usage:
    docker compose exec sharp-seeker python /app/scripts/analyze_combos.py
"""

import sqlite3
import time
from collections import defaultdict

DB = "/app/data/sharp_seeker.db"

CURRENT_BEST_COMBOS = {
    "reverse_line:basketball_nba:spreads",
    "pinnacle_divergence:icehockey_nhl:totals",
    "reverse_line:basketball_ncaab:h2h",
    "steam_move:basketball_nba:spreads",
    "pinnacle_divergence:basketball_ncaab:spreads",
    "pinnacle_divergence:basketball_nba:totals",
    "pinnacle_divergence:basketball_ncaab:h2h",
    "steam_move:basketball_nba:totals",
}

SPORT_SHORT = {
    "basketball_nba": "NBA",
    "basketball_ncaab": "NCAAB",
    "icehockey_nhl": "NHL",
    "baseball_mlb": "MLB",
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


def main():
    conn = connect()

    signals = conn.execute("""
        SELECT signal_type, sport_key, market_key, signal_at, result
        FROM signal_results
        WHERE result IS NOT NULL
        ORDER BY signal_at
    """).fetchall()
    signals = [dict(r) for r in signals]
    conn.close()

    if not signals:
        print("No resolved signals found.")
        return

    # Parse dates for recency splits
    for s in signals:
        s["date"] = s["signal_at"][:10] if s.get("signal_at") else ""

    # Find date cutoffs
    all_dates = sorted(set(s["date"] for s in signals))
    if len(all_dates) >= 14:
        last_14d = all_dates[-14]
    else:
        last_14d = all_dates[0]
    if len(all_dates) >= 7:
        last_7d = all_dates[-7]
    else:
        last_7d = all_dates[0]

    # Group by combo
    combos = defaultdict(list)
    for s in signals:
        key = "{t}:{sp}:{mk}".format(
            t=s["signal_type"], sp=s.get("sport_key", "unknown"), mk=s["market_key"],
        )
        combos[key].append(s)

    def _record(sigs):
        w = sum(1 for s in sigs if s["result"] == "won")
        l = sum(1 for s in sigs if s["result"] == "lost")
        return w, l

    print("=" * 90)
    print("  COMBO PERFORMANCE ANALYSIS")
    print("  Total resolved signals: {n}".format(n=len(signals)))
    print("=" * 90)

    # ═══════════════════════════════════════════════════════════════
    # 1. ALL COMBOS — ranked by win rate (min 10 decided)
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 90)
    print("  ALL COMBOS — ALL TIME (min 10 decided, ranked by win%)")
    print("=" * 90)
    print("  {combo:<45s} {w:>4s} {l:>4s} {n:>5s} {wr:>7s}  {status}".format(
        combo="Combo", w="W", l="L", n="Dec", wr="Win%", status="Status",
    ))
    print("  " + "-" * 85)

    ranked = []
    for key, sigs in combos.items():
        w, l = _record(sigs)
        d = w + l
        if d < 10:
            continue
        rate = w / d * 100
        is_best = key in CURRENT_BEST_COMBOS
        ranked.append((key, w, l, d, rate, is_best))

    ranked.sort(key=lambda x: -x[4])

    for key, w, l, d, rate, is_best in ranked:
        # Short display name
        parts = key.split(":")
        short = "{t}:{sp}:{mk}".format(
            t=SIGNAL_SHORT.get(parts[0], parts[0]),
            sp=SPORT_SHORT.get(parts[1], parts[1]),
            mk=parts[2],
        )

        markers = []
        if is_best:
            markers.append("CURRENT")
        if rate >= 55 and d >= 15 and not is_best:
            markers.append("CANDIDATE")
        if is_best and rate < 50:
            markers.append("REVIEW")
        if rate >= 58:
            markers.append("***")
        elif rate < 48:
            markers.append("!!!")

        status = "  [{m}]".format(m=", ".join(markers)) if markers else ""

        print("  {short:<45s} {w:>4d} {l:>4d} {n:>5d} {wr}  {s}".format(
            short=short, w=w, l=l, n=d, wr=_wr(w, l), s=status,
        ))

    # ═══════════════════════════════════════════════════════════════
    # 2. CURRENT BEST COMBOS AUDIT
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 90)
    print("  CURRENT BEST COMBOS AUDIT")
    print("=" * 90)

    overall_w = sum(1 for s in signals if s["result"] == "won")
    overall_l = sum(1 for s in signals if s["result"] == "lost")
    overall_rate = overall_w / (overall_w + overall_l) * 100

    print("  Overall avg: {r:.1f}%".format(r=overall_rate))
    print()

    # Performance of best combo signals vs others
    best_sigs = []
    other_sigs = []
    for s in signals:
        key = "{t}:{sp}:{mk}".format(
            t=s["signal_type"], sp=s.get("sport_key", "unknown"), mk=s["market_key"],
        )
        if key in CURRENT_BEST_COMBOS:
            best_sigs.append(s)
        else:
            other_sigs.append(s)

    bw, bl = _record(best_sigs)
    ow, ol = _record(other_sigs)
    print("  Best combo signals:  {w}-{l} ({wr})  n={n}".format(
        w=bw, l=bl, wr=_wr(bw, bl), n=bw + bl,
    ))
    print("  Other signals:       {w}-{l} ({wr})  n={n}".format(
        w=ow, l=ol, wr=_wr(ow, ol), n=ow + ol,
    ))
    print()

    print("  Per-combo breakdown:")
    for key in sorted(CURRENT_BEST_COMBOS):
        sigs = combos.get(key, [])
        w, l = _record(sigs)
        d = w + l
        parts = key.split(":")
        short = "{t}:{sp}:{mk}".format(
            t=SIGNAL_SHORT.get(parts[0], parts[0]),
            sp=SPORT_SHORT.get(parts[1], parts[1]),
            mk=parts[2],
        )
        rate = w / d * 100 if d else 0
        diff = rate - overall_rate if d else 0
        status = "OK" if d >= 10 and rate > overall_rate else "REVIEW"
        if d < 10:
            status = "LOW-N"

        # Recent trend (last 14 days)
        recent = [s for s in sigs if s["date"] >= last_14d]
        rw, rl = _record(recent)
        rd = rw + rl

        print("    {short:<30s} All: {w:>3d}-{l:<3d} ({wr})  n={n:>3d}  d={d:+.0f}pp  [{s}]   14d: {rw}-{rl} ({rwr})".format(
            short=short, w=w, l=l, wr=_wr(w, l), n=d,
            d=diff, s=status,
            rw=rw, rl=rl, rwr=_wr(rw, rl),
        ))

    # ═══════════════════════════════════════════════════════════════
    # 3. CANDIDATES TO ADD
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 90)
    print("  CANDIDATES TO ADD (>= 55%, n >= 15, not already best combo)")
    print("=" * 90)

    candidates = []
    for key, w, l, d, rate, is_best in ranked:
        if not is_best and rate >= 55 and d >= 15:
            # Check recent trend
            recent = [s for s in combos[key] if s["date"] >= last_14d]
            rw, rl = _record(recent)
            candidates.append((key, w, l, d, rate, rw, rl))

    if candidates:
        for key, w, l, d, rate, rw, rl in candidates:
            parts = key.split(":")
            short = "{t}:{sp}:{mk}".format(
                t=SIGNAL_SHORT.get(parts[0], parts[0]),
                sp=SPORT_SHORT.get(parts[1], parts[1]),
                mk=parts[2],
            )
            print("  {short:<35s} All: {w:>3d}-{l:<3d} ({wr})  n={n}   14d: {rw}-{rl} ({rwr})".format(
                short=short, w=w, l=l, wr=_wr(w, l), n=d,
                rw=rw, rl=rl, rwr=_wr(rw, rl),
            ))
    else:
        print("  (none)")

    # ═══════════════════════════════════════════════════════════════
    # 4. ALL COMBOS WITH SMALL SAMPLE (5-9 decided)
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 90)
    print("  SMALL SAMPLE COMBOS (5-9 decided — watch list)")
    print("=" * 90)

    small = []
    for key, sigs in combos.items():
        w, l = _record(sigs)
        d = w + l
        if 5 <= d < 10:
            rate = w / d * 100
            small.append((key, w, l, d, rate))

    small.sort(key=lambda x: -x[4])
    for key, w, l, d, rate in small:
        parts = key.split(":")
        short = "{t}:{sp}:{mk}".format(
            t=SIGNAL_SHORT.get(parts[0], parts[0]),
            sp=SPORT_SHORT.get(parts[1], parts[1]),
            mk=parts[2],
        )
        is_best = key in CURRENT_BEST_COMBOS
        marker = " [CURRENT]" if is_best else ""
        print("  {short:<35s} {w:>3d}-{l:<3d} ({wr})  n={n}{m}".format(
            short=short, w=w, l=l, wr=_wr(w, l), n=d, m=marker,
        ))

    print()
    print("=" * 90)
    print("  ANALYSIS COMPLETE")
    print("=" * 90)


if __name__ == "__main__":
    main()
