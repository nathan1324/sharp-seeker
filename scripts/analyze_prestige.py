"""Analyze additional signal quality dimensions for a tiered badge system.

Tests which dimensions actually differentiate winners from losers:
  1. Combo + Hour overlap (both vs either)
  2. Hold (sharp vs wide)
  3. Signal convergence (multiple detectors on same event+outcome)
  4. Time to game start
  5. Day of week
  6. Book identity (which US book)

Usage:
    docker compose exec sharp-seeker python /app/scripts/analyze_prestige.py
"""

import json
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone

DB = "/app/data/sharp_seeker.db"

BEST_COMBOS = {
    "reverse_line:basketball_nba:spreads",
    "pinnacle_divergence:icehockey_nhl:totals",
    "reverse_line:basketball_ncaab:h2h",
    "steam_move:basketball_nba:spreads",
    "pinnacle_divergence:basketball_ncaab:spreads",
    "pinnacle_divergence:basketball_nba:totals",
    "pinnacle_divergence:basketball_ncaab:h2h",
    "steam_move:basketball_nba:totals",
}

BEST_HOURS = {
    "pinnacle_divergence": {5, 12, 14, 16, 17},
    "reverse_line": {8, 12},
    "rapid_change": {18, 19},
    "steam_move": {9, 11, 13},
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

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


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


def _record(sigs):
    w = sum(1 for s in sigs if s["result"] == "won")
    l = sum(1 for s in sigs if s["result"] == "lost")
    return w, l


def _print_row(label, sigs, indent=2):
    w, l = _record(sigs)
    d = w + l
    if d == 0:
        return
    pad = " " * indent
    marker = ""
    rate = w / d * 100
    if d >= 10 and rate >= 58:
        marker = " ***"
    elif d >= 10 and rate < 45:
        marker = " !!!"
    print("{pad}{label:<40s} {w:>4d}-{l:<4d} ({wr})  n={n}{m}".format(
        pad=pad, label=label, w=w, l=l, wr=_wr(w, l), n=d, m=marker,
    ))


def main():
    conn = connect()

    signals = conn.execute("""
        SELECT signal_type, sport_key, market_key, outcome_name,
               event_id, signal_at, signal_strength, result,
               details_json
        FROM signal_results
        WHERE result IS NOT NULL
        ORDER BY signal_at
    """).fetchall()
    signals = [dict(r) for r in signals]
    conn.close()

    if not signals:
        print("No resolved signals found.")
        return

    # Enrich signals with derived fields
    for s in signals:
        sa = s.get("signal_at", "")

        # MST hour
        try:
            utc_h = int(sa[11:13])
            s["mst_hour"] = (utc_h - 7) % 24
        except (ValueError, IndexError):
            s["mst_hour"] = -1

        # Combo key
        s["combo"] = "{t}:{sp}:{mk}".format(
            t=s["signal_type"],
            sp=s.get("sport_key", "unknown"),
            mk=s["market_key"],
        )
        s["is_best_combo"] = s["combo"] in BEST_COMBOS

        # Best hour
        hours = BEST_HOURS.get(s["signal_type"], set())
        s["is_best_hour"] = s["mst_hour"] in hours

        # Parse details
        details = {}
        if s.get("details_json"):
            try:
                details = json.loads(s["details_json"])
            except (json.JSONDecodeError, TypeError):
                pass
        s["_details"] = details

        # Hold
        s["us_hold"] = details.get("us_hold")

        # Book
        us_book = details.get("us_book", "")
        if not us_book:
            vb = details.get("value_books", [])
            if vb:
                us_book = vb[0].get("bookmaker", "")
        s["us_book"] = us_book

        # Day of week
        try:
            dt = datetime.fromisoformat(sa)
            s["day_of_week"] = dt.weekday()  # 0=Mon, 6=Sun
        except (ValueError, TypeError):
            s["day_of_week"] = -1

        # Time to game start (hours)
        commence = details.get("commence_time", "")
        if not commence:
            # Try from the signal itself (some detectors store it differently)
            commence = ""
        # We need commence_time from signal_results but it's not stored there
        # So we skip this for now unless we can get it from details
        s["hours_to_game"] = None

    # Also get commence_time from odds_snapshots for convergence analysis
    # Build event → signals mapping for convergence
    event_signals = defaultdict(list)
    for s in signals:
        key = (s["event_id"], s["market_key"], s["outcome_name"])
        event_signals[key].append(s)

    # Mark convergence: multiple signal types on same event+market+outcome
    for s in signals:
        key = (s["event_id"], s["market_key"], s["outcome_name"])
        all_types = set(sig["signal_type"] for sig in event_signals[key])
        s["convergence_count"] = len(all_types)
        s["has_convergence"] = len(all_types) >= 2

    total = len(signals)
    overall_w, overall_l = _record(signals)
    overall_rate = overall_w / (overall_w + overall_l) * 100

    print("=" * 80)
    print("  PRESTIGE DIMENSION ANALYSIS")
    print("  Total: {n} signals, {w}-{l} ({wr}) overall".format(
        n=total, w=overall_w, l=overall_l, wr=_wr(overall_w, overall_l),
    ))
    print("=" * 80)

    # ═══════════════════════════════════════════════════════════════
    # 1. COMBO + HOUR OVERLAP
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 80)
    print("  1. COMBO + HOUR OVERLAP")
    print("     Do signals matching BOTH qualifiers outperform?")
    print("=" * 80)

    both = [s for s in signals if s["is_best_combo"] and s["is_best_hour"]]
    combo_only = [s for s in signals if s["is_best_combo"] and not s["is_best_hour"]]
    hour_only = [s for s in signals if not s["is_best_combo"] and s["is_best_hour"]]
    neither = [s for s in signals if not s["is_best_combo"] and not s["is_best_hour"]]

    _print_row("Both combo AND hour", both)
    _print_row("Combo only (no hour match)", combo_only)
    _print_row("Hour only (no combo match)", hour_only)
    _print_row("Neither", neither)

    # ═══════════════════════════════════════════════════════════════
    # 2. HOLD AS QUALIFIER
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 80)
    print("  2. HOLD (VIG) AS QUALIFIER")
    print("     Lower hold = sharper pricing = cleaner edge?")
    print("=" * 80)

    has_hold = [s for s in signals if s["us_hold"] is not None]
    if has_hold:
        hold_bands = [
            (0.0, 0.035, "< 3.5% (very sharp)"),
            (0.035, 0.045, "3.5-4.5% (sharp)"),
            (0.045, 0.050, "4.5-5.0% (average)"),
            (0.050, 0.055, "5.0-5.5% (wide)"),
            (0.055, 1.0, "5.5%+ (very wide)"),
        ]
        for lo, hi, label in hold_bands:
            band = [s for s in has_hold if lo <= s["us_hold"] < hi]
            _print_row(label, band)

        # Sharp hold + best combo
        print()
        print("  Cross-tab: Hold x Best Combo")
        sharp_best = [s for s in has_hold if s["us_hold"] < 0.045 and s["is_best_combo"]]
        sharp_other = [s for s in has_hold if s["us_hold"] < 0.045 and not s["is_best_combo"]]
        wide_best = [s for s in has_hold if s["us_hold"] >= 0.045 and s["is_best_combo"]]
        wide_other = [s for s in has_hold if s["us_hold"] >= 0.045 and not s["is_best_combo"]]

        _print_row("Sharp hold + best combo", sharp_best, indent=4)
        _print_row("Sharp hold + other combo", sharp_other, indent=4)
        _print_row("Wide hold + best combo", wide_best, indent=4)
        _print_row("Wide hold + other combo", wide_other, indent=4)

        # Triple: sharp hold + best combo + best hour
        print()
        print("  Triple qualifier: Sharp hold + best combo + best hour")
        triple = [s for s in has_hold
                  if s["us_hold"] < 0.045 and s["is_best_combo"] and s["is_best_hour"]]
        double_no_hour = [s for s in has_hold
                          if s["us_hold"] < 0.045 and s["is_best_combo"] and not s["is_best_hour"]]
        _print_row("All three (hold + combo + hour)", triple, indent=4)
        _print_row("Sharp hold + combo (no hour)", double_no_hour, indent=4)
    else:
        print("  No hold data available (hold feature may be too recent).")

    # ═══════════════════════════════════════════════════════════════
    # 3. SIGNAL CONVERGENCE
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 80)
    print("  3. SIGNAL CONVERGENCE")
    print("     Multiple detector types firing on same event+outcome")
    print("=" * 80)

    converged = [s for s in signals if s["has_convergence"]]
    single = [s for s in signals if not s["has_convergence"]]

    _print_row("2+ signal types on same outcome", converged)
    _print_row("Single signal type only", single)

    # Break down by convergence count
    for count in sorted(set(s["convergence_count"] for s in signals)):
        if count < 2:
            continue
        label = "{n} signal types converged".format(n=count)
        group = [s for s in signals if s["convergence_count"] == count]
        _print_row(label, group, indent=4)

    # Convergence + best combo
    print()
    print("  Cross-tab: Convergence x Best Combo")
    conv_best = [s for s in signals if s["has_convergence"] and s["is_best_combo"]]
    conv_other = [s for s in signals if s["has_convergence"] and not s["is_best_combo"]]
    single_best = [s for s in signals if not s["has_convergence"] and s["is_best_combo"]]
    single_other = [s for s in signals if not s["has_convergence"] and not s["is_best_combo"]]

    _print_row("Converged + best combo", conv_best, indent=4)
    _print_row("Converged + other combo", conv_other, indent=4)
    _print_row("Single + best combo", single_best, indent=4)
    _print_row("Single + other combo", single_other, indent=4)

    # ═══════════════════════════════════════════════════════════════
    # 4. DAY OF WEEK
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 80)
    print("  4. DAY OF WEEK")
    print("=" * 80)

    for dow in range(7):
        day_sigs = [s for s in signals if s["day_of_week"] == dow]
        _print_row(DAYS[dow], day_sigs)

    # Weekend vs weekday
    print()
    weekend = [s for s in signals if s["day_of_week"] in (5, 6)]
    weekday = [s for s in signals if s["day_of_week"] in (0, 1, 2, 3, 4)]
    _print_row("Weekday (Mon-Fri)", weekday)
    _print_row("Weekend (Sat-Sun)", weekend)

    # ═══════════════════════════════════════════════════════════════
    # 5. BOOK IDENTITY
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 80)
    print("  5. BOOK IDENTITY")
    print("     Which US book's signals win more?")
    print("=" * 80)

    books = sorted(set(s["us_book"] for s in signals if s["us_book"]))
    for book in books:
        book_sigs = [s for s in signals if s["us_book"] == book]
        _print_row(book.title(), book_sigs)

    # Book x signal type
    print()
    print("  By book + signal type:")
    for book in books:
        if not book:
            continue
        book_sigs = [s for s in signals if s["us_book"] == book]
        for st in sorted(set(s["signal_type"] for s in book_sigs)):
            st_sigs = [s for s in book_sigs if s["signal_type"] == st]
            w, l = _record(st_sigs)
            if w + l < 10:
                continue
            label = "{b} + {t}".format(b=book.title(), t=SIGNAL_SHORT.get(st, st))
            _print_row(label, st_sigs, indent=4)

    # ═══════════════════════════════════════════════════════════════
    # 6. STRENGTH BANDS
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 80)
    print("  6. STRENGTH BANDS")
    print("     Does strength predict outcomes?")
    print("=" * 80)

    str_bands = [
        (0.0, 0.35, "< 35%"),
        (0.35, 0.50, "35-50%"),
        (0.50, 0.60, "50-60%"),
        (0.60, 0.70, "60-70%"),
        (0.70, 0.80, "70-80%"),
        (0.80, 1.01, "80%+"),
    ]
    for lo, hi, label in str_bands:
        band = [s for s in signals if lo <= s["signal_strength"] < hi]
        _print_row(label, band)

    # ═══════════════════════════════════════════════════════════════
    # 7. QUALIFIER STACKING SUMMARY
    # ═══════════════════════════════════════════════════════════════
    print()
    print("=" * 80)
    print("  7. QUALIFIER STACKING SUMMARY")
    print("     How many qualifiers does a signal meet?")
    print("     Qualifiers: best combo, best hour, sharp hold (<4.5%),")
    print("     convergence (2+ types)")
    print("=" * 80)

    for s in signals:
        qualifiers = 0
        if s["is_best_combo"]:
            qualifiers += 1
        if s["is_best_hour"]:
            qualifiers += 1
        if s["us_hold"] is not None and s["us_hold"] < 0.045:
            qualifiers += 1
        if s["has_convergence"]:
            qualifiers += 1
        s["qualifier_count"] = qualifiers

    for q in range(5):
        group = [s for s in signals if s["qualifier_count"] == q]
        if not group:
            continue
        label = "{n} qualifier{s}".format(n=q, s="s" if q != 1 else "")
        _print_row(label, group)

    # Cumulative: >= N qualifiers
    print()
    print("  Cumulative (>= N qualifiers):")
    for q in range(1, 5):
        group = [s for s in signals if s["qualifier_count"] >= q]
        if not group:
            continue
        label = ">= {n} qualifier{s}".format(n=q, s="s" if q != 1 else "")
        _print_row(label, group, indent=4)

    print()
    print("=" * 80)
    print("  ANALYSIS COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
