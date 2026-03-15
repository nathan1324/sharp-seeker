"""Analyze PD win rate by US book hold (vig) and market confirmation.

Reconstructs hold and confirmation from odds_snapshots at signal time.

Usage:
    docker compose exec sharp-seeker python /app/scripts/analyze_pd_hold.py
    docker compose exec sharp-seeker python /app/scripts/analyze_pd_hold.py 2026-03-01
"""

import json
import sqlite3
import sys
import time
from collections import defaultdict

DB = "/app/data/sharp_seeker.db"
PINNACLE_KEY = "pinnacle"
US_BOOKS = {"draftkings", "fanduel", "betmgm", "caesars", "williamhill_us"}


def american_to_implied_prob(price):
    if price >= 100:
        return 100.0 / (price + 100.0)
    else:
        return abs(price) / (abs(price) + 100.0)


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


def compute_hold(by_bm, bookmaker, market_key, outcome_name):
    """Compute hold (overround) for a bookmaker on a market.

    Returns float (e.g. 0.048 = 4.8% hold) or None if other side missing.
    """
    bm_rows = by_bm.get(bookmaker, [])
    if not bm_rows:
        return None

    # Determine opposite outcome
    if market_key == "totals":
        other = "Under" if outcome_name == "Over" else "Over"
    else:
        # h2h/spreads: find the other team name
        other = None
        for r in bm_rows:
            if r["market_key"] == market_key and r["outcome_name"] != outcome_name:
                other = r["outcome_name"]
                break
        if other is None:
            return None

    this_price = None
    other_price = None
    for r in bm_rows:
        if r["market_key"] != market_key:
            continue
        if r["outcome_name"] == outcome_name:
            this_price = r["price"]
        elif r["outcome_name"] == other:
            other_price = r["price"]

    if this_price is None or other_price is None:
        return None

    return american_to_implied_prob(this_price) + american_to_implied_prob(other_price) - 1.0


def check_confirmation(by_bm, market_key, outcome_name, pinnacle_value,
                        signaled_book, is_h2h):
    """Check if another US book aligns with Pinnacle's number.

    For h2h: implied prob within 1%.
    For spreads/totals: point within threshold (use 0.5 as proxy).
    """
    confirming = []
    for bm, rows in by_bm.items():
        if bm not in US_BOOKS or bm == signaled_book:
            continue
        for r in rows:
            if r["market_key"] != market_key or r["outcome_name"] != outcome_name:
                continue
            if is_h2h:
                bm_prob = american_to_implied_prob(r["price"])
                pin_prob = american_to_implied_prob(pinnacle_value)
                if abs(bm_prob - pin_prob) <= 0.01:
                    confirming.append(bm)
            else:
                if r["point"] is not None:
                    if abs(r["point"] - pinnacle_value) < 0.5:
                        confirming.append(bm)
    return len(confirming) > 0, confirming


def _wr(w, l):
    """Format win rate string."""
    decided = w + l
    if decided == 0:
        return "N/A"
    return "{p:.0f}%".format(p=w / decided * 100)


def main():
    since = sys.argv[1] if len(sys.argv) > 1 else None
    conn = connect()

    # 1. Get all resolved PD signals
    where = "WHERE signal_type = 'pinnacle_divergence' AND result IS NOT NULL"
    params = []
    if since:
        where += " AND signal_at >= ?"
        params.append(since)

    signals = conn.execute(
        "SELECT * FROM signal_results {w} ORDER BY signal_at".format(w=where),
        params,
    ).fetchall()

    if not signals:
        print("No resolved PD signals found.")
        return

    since_label = " (since {d})".format(d=since) if since else ""
    print("=== PD HOLD & CONFIRMATION ANALYSIS{l} ===".format(l=since_label))
    print("Total resolved PD signals: {n}".format(n=len(signals)))
    print()

    # 2. For each signal, reconstruct snapshot and compute hold + confirmation
    results = []
    skipped = 0

    for sig in signals:
        sig = dict(sig)
        details = {}
        if sig.get("details_json"):
            try:
                details = json.loads(sig["details_json"])
            except (json.JSONDecodeError, TypeError):
                pass

        us_book = details.get("us_book", "")
        pinnacle_value = details.get("pinnacle_value")
        if not us_book or pinnacle_value is None:
            skipped += 1
            continue

        # Get all snapshot rows at signal time
        snap_rows = conn.execute("""
            SELECT bookmaker_key, market_key, outcome_name, price, point
            FROM odds_snapshots
            WHERE event_id = ? AND fetched_at = ?
        """, (sig["event_id"], sig["signal_at"])).fetchall()

        if not snap_rows:
            # Try closest fetched_at before signal_at
            snap_rows = conn.execute("""
                SELECT bookmaker_key, market_key, outcome_name, price, point
                FROM odds_snapshots
                WHERE event_id = ? AND fetched_at = (
                    SELECT MAX(fetched_at) FROM odds_snapshots
                    WHERE event_id = ? AND fetched_at <= ?
                )
            """, (sig["event_id"], sig["event_id"], sig["signal_at"])).fetchall()

        if not snap_rows:
            skipped += 1
            continue

        # Index by bookmaker
        by_bm = {}
        for r in snap_rows:
            r = dict(r)
            by_bm.setdefault(r["bookmaker_key"], []).append(r)

        is_h2h = sig["market_key"] == "h2h"

        # Compute holds
        us_hold = compute_hold(by_bm, us_book, sig["market_key"], sig["outcome_name"])
        pin_hold = compute_hold(by_bm, PINNACLE_KEY, sig["market_key"], sig["outcome_name"])

        # Check confirmation
        confirmed, confirming_books = check_confirmation(
            by_bm, sig["market_key"], sig["outcome_name"],
            pinnacle_value, us_book, is_h2h,
        )

        results.append({
            "result": sig["result"],
            "sport_key": sig.get("sport_key", "unknown"),
            "market_key": sig["market_key"],
            "us_book": us_book,
            "us_hold": us_hold,
            "pin_hold": pin_hold,
            "confirmed": confirmed,
            "strength": sig["signal_strength"],
        })

    conn.close()

    if skipped:
        print("Skipped {n} signals (missing details or snapshots)".format(n=skipped))
    print("Analyzed {n} signals".format(n=len(results)))
    print()

    # --- HOLD ANALYSIS ---
    hold_results = [r for r in results if r["us_hold"] is not None]
    if hold_results:
        print("=" * 55)
        print("=== PD WIN RATE BY US BOOK HOLD (VIG) ===")
        print("=" * 55)

        hold_bands = [
            (0.0, 0.025, "< 2.5%"),
            (0.025, 0.035, "2.5-3.5%"),
            (0.035, 0.045, "3.5-4.5%"),
            (0.045, 0.055, "4.5-5.5%"),
            (0.055, 0.070, "5.5-7.0%"),
            (0.070, 1.0, "7.0%+"),
        ]

        print("{band:<12s} {w:>5s} {l:>5s} {n:>5s} {pct:>7s}  {avg_hold:>8s}".format(
            band="Hold Band", w="W", l="L", n="Total", pct="Win%", avg_hold="Avg Hold",
        ))
        print("-" * 50)

        for lo, hi, label in hold_bands:
            band = [r for r in hold_results if lo <= r["us_hold"] < hi]
            if not band:
                continue
            w = sum(1 for r in band if r["result"] == "won")
            l_count = sum(1 for r in band if r["result"] == "lost")
            decided = w + l_count
            avg_h = sum(r["us_hold"] for r in band) / len(band)
            marker = " ***" if decided >= 10 and w / max(decided, 1) >= 0.58 else ""
            marker = " !!!" if decided >= 10 and w / max(decided, 1) < 0.50 else marker
            print("{label:<12s} {w:>5d} {l:>5d} {n:>5d} {pct:>7s}  {avg:>7.1f}%{m}".format(
                label=label, w=w, l=l_count, n=decided,
                pct=_wr(w, l_count), avg=avg_h * 100, m=marker,
            ))

        # Cumulative: "at or below X% hold"
        print()
        print("=== CUMULATIVE: HOLD AT OR BELOW THRESHOLD ===")
        print("{thresh:<15s} {w:>5s} {l:>5s} {n:>5s} {pct:>7s}".format(
            thresh="Max Hold", w="W", l="L", n="Total", pct="Win%",
        ))
        print("-" * 42)
        for t in [25, 30, 35, 40, 45, 50, 55, 60, 70]:
            t_val = t / 1000.0  # e.g., 25 → 0.025 = 2.5%
            below = [r for r in hold_results if r["us_hold"] <= t_val]
            if not below:
                continue
            w = sum(1 for r in below if r["result"] == "won")
            l_count = sum(1 for r in below if r["result"] == "lost")
            decided = w + l_count
            print("<= {t:.1f}%{pad} {w:>5d} {l:>5d} {n:>5d} {pct:>7s}".format(
                t=t / 10, pad=" " * (10 - len("{:.1f}".format(t / 10))),
                w=w, l=l_count, n=decided, pct=_wr(w, l_count),
            ))

        # Hold by sport
        print()
        print("=== HOLD BY SPORT ===")
        sport_short = {
            "basketball_nba": "NBA",
            "basketball_ncaab": "NCAAB",
            "icehockey_nhl": "NHL",
            "baseball_mlb": "MLB",
        }
        sports = sorted(set(r["sport_key"] for r in hold_results))
        for sp in sports:
            sp_rows = [r for r in hold_results if r["sport_key"] == sp]
            sp_label = sport_short.get(sp, sp)
            print("\n  {sp}:".format(sp=sp_label))
            for lo, hi, label in hold_bands:
                band = [r for r in sp_rows if lo <= r["us_hold"] < hi]
                if not band:
                    continue
                w = sum(1 for r in band if r["result"] == "won")
                l_count = sum(1 for r in band if r["result"] == "lost")
                decided = w + l_count
                marker = " ***" if decided >= 5 and w / max(decided, 1) >= 0.58 else ""
                marker = " !!!" if decided >= 5 and w / max(decided, 1) < 0.50 else marker
                print("    {label:<12s} {w:>3d}-{l:<3d} ({pct})  n={n}{m}".format(
                    label=label, w=w, l=l_count, pct=_wr(w, l_count), n=decided, m=marker,
                ))

        # Hold by market
        print()
        print("=== HOLD BY MARKET ===")
        markets = sorted(set(r["market_key"] for r in hold_results))
        for mk in markets:
            mk_rows = [r for r in hold_results if r["market_key"] == mk]
            print("\n  {mk}:".format(mk=mk))
            for lo, hi, label in hold_bands:
                band = [r for r in mk_rows if lo <= r["us_hold"] < hi]
                if not band:
                    continue
                w = sum(1 for r in band if r["result"] == "won")
                l_count = sum(1 for r in band if r["result"] == "lost")
                decided = w + l_count
                marker = " ***" if decided >= 5 and w / max(decided, 1) >= 0.58 else ""
                marker = " !!!" if decided >= 5 and w / max(decided, 1) < 0.50 else marker
                print("    {label:<12s} {w:>3d}-{l:<3d} ({pct})  n={n}{m}".format(
                    label=label, w=w, l=l_count, pct=_wr(w, l_count), n=decided, m=marker,
                ))
    else:
        print("No signals with computable hold found.")

    # --- CONFIRMATION ANALYSIS ---
    print()
    print("=" * 55)
    print("=== PD WIN RATE BY MARKET CONFIRMATION ===")
    print("=" * 55)
    print("Confirmed = another US book already at Pinnacle's number")
    print()

    confirmed_rows = [r for r in results if r["confirmed"]]
    unconfirmed_rows = [r for r in results if not r["confirmed"]]

    cw = sum(1 for r in confirmed_rows if r["result"] == "won")
    cl = sum(1 for r in confirmed_rows if r["result"] == "lost")
    uw = sum(1 for r in unconfirmed_rows if r["result"] == "won")
    ul = sum(1 for r in unconfirmed_rows if r["result"] == "lost")

    print("{status:<15s} {w:>5s} {l:>5s} {n:>5s} {pct:>7s}".format(
        status="Status", w="W", l="L", n="Total", pct="Win%",
    ))
    print("-" * 40)
    print("{s:<15s} {w:>5d} {l:>5d} {n:>5d} {pct:>7s}".format(
        s="Confirmed", w=cw, l=cl, n=cw + cl, pct=_wr(cw, cl),
    ))
    print("{s:<15s} {w:>5d} {l:>5d} {n:>5d} {pct:>7s}".format(
        s="Not confirmed", w=uw, l=ul, n=uw + ul, pct=_wr(uw, ul),
    ))

    # Confirmation by sport
    print()
    print("=== CONFIRMATION BY SPORT ===")
    sports = sorted(set(r["sport_key"] for r in results))
    for sp in sports:
        sp_label = sport_short.get(sp, sp)
        sp_conf = [r for r in confirmed_rows if r["sport_key"] == sp]
        sp_unconf = [r for r in unconfirmed_rows if r["sport_key"] == sp]
        cw2 = sum(1 for r in sp_conf if r["result"] == "won")
        cl2 = sum(1 for r in sp_conf if r["result"] == "lost")
        uw2 = sum(1 for r in sp_unconf if r["result"] == "won")
        ul2 = sum(1 for r in sp_unconf if r["result"] == "lost")
        if (cw2 + cl2) == 0 and (uw2 + ul2) == 0:
            continue
        print("\n  {sp}:".format(sp=sp_label))
        if cw2 + cl2 > 0:
            print("    Confirmed:      {w:>3d}-{l:<3d} ({pct})  n={n}".format(
                w=cw2, l=cl2, pct=_wr(cw2, cl2), n=cw2 + cl2,
            ))
        if uw2 + ul2 > 0:
            print("    Not confirmed:  {w:>3d}-{l:<3d} ({pct})  n={n}".format(
                w=uw2, l=ul2, pct=_wr(uw2, ul2), n=uw2 + ul2,
            ))

    # --- CROSS-TAB: HOLD x CONFIRMATION ---
    if hold_results:
        print()
        print("=" * 55)
        print("=== CROSS-TAB: HOLD x CONFIRMATION ===")
        print("=" * 55)

        categories = [
            ("Low hold + confirmed", lambda r: r["us_hold"] is not None and r["us_hold"] < 0.04 and r["confirmed"]),
            ("Low hold + unconfirmed", lambda r: r["us_hold"] is not None and r["us_hold"] < 0.04 and not r["confirmed"]),
            ("High hold + confirmed", lambda r: r["us_hold"] is not None and r["us_hold"] >= 0.04 and r["confirmed"]),
            ("High hold + unconfirmed", lambda r: r["us_hold"] is not None and r["us_hold"] >= 0.04 and not r["confirmed"]),
        ]

        print("{cat:<25s} {w:>5s} {l:>5s} {n:>5s} {pct:>7s}".format(
            cat="Category", w="W", l="L", n="Total", pct="Win%",
        ))
        print("-" * 50)

        for label, pred in categories:
            cat_rows = [r for r in results if pred(r)]
            w = sum(1 for r in cat_rows if r["result"] == "won")
            l_count = sum(1 for r in cat_rows if r["result"] == "lost")
            decided = w + l_count
            marker = " ***" if decided >= 5 and w / max(decided, 1) >= 0.58 else ""
            marker = " !!!" if decided >= 5 and w / max(decided, 1) < 0.50 else marker
            print("{label:<25s} {w:>5d} {l:>5d} {n:>5d} {pct:>7s}{m}".format(
                label=label, w=w, l=l_count, n=decided,
                pct=_wr(w, l_count), m=marker,
            ))

        # Cross-tab by sport
        print()
        print("=== CROSS-TAB BY SPORT ===")
        for sp in sports:
            sp_label = sport_short.get(sp, sp)
            sp_rows = [r for r in results if r["sport_key"] == sp]
            if not sp_rows:
                continue
            print("\n  {sp}:".format(sp=sp_label))
            for label, pred in categories:
                cat_rows = [r for r in sp_rows if pred(r)]
                w = sum(1 for r in cat_rows if r["result"] == "won")
                l_count = sum(1 for r in cat_rows if r["result"] == "lost")
                decided = w + l_count
                if decided == 0:
                    continue
                marker = " ***" if decided >= 5 and w / max(decided, 1) >= 0.58 else ""
                marker = " !!!" if decided >= 5 and w / max(decided, 1) < 0.50 else marker
                print("    {label:<25s} {w:>3d}-{l:<3d} ({pct})  n={n}{m}".format(
                    label=label, w=w, l=l_count, pct=_wr(w, l_count), n=decided, m=marker,
                ))

    # --- PINNACLE HOLD COMPARISON ---
    pin_hold_rows = [r for r in results if r["pin_hold"] is not None and r["us_hold"] is not None]
    if pin_hold_rows:
        print()
        print("=" * 55)
        print("=== PINNACLE vs US BOOK HOLD ===")
        print("=" * 55)
        avg_pin = sum(r["pin_hold"] for r in pin_hold_rows) / len(pin_hold_rows)
        avg_us = sum(r["us_hold"] for r in pin_hold_rows) / len(pin_hold_rows)
        print("Average Pinnacle hold: {h:.2f}%".format(h=avg_pin * 100))
        print("Average US book hold:  {h:.2f}%".format(h=avg_us * 100))
        print("Average hold spread:   {h:.2f}%".format(h=(avg_us - avg_pin) * 100))

    # --- SUMMARY STATS ---
    print()
    print("=" * 55)
    print("=== SUMMARY ===")
    print("=" * 55)
    if hold_results:
        holds = [r["us_hold"] for r in hold_results]
        print("Hold range: {lo:.1f}% - {hi:.1f}%".format(
            lo=min(holds) * 100, hi=max(holds) * 100,
        ))
        print("Hold median: {m:.1f}%".format(
            m=sorted(holds)[len(holds) // 2] * 100,
        ))
    total_conf = len(confirmed_rows)
    total_unconf = len(unconfirmed_rows)
    print("Confirmed: {c} ({pct:.0f}%)  |  Not confirmed: {u} ({pct2:.0f}%)".format(
        c=total_conf, pct=total_conf / max(len(results), 1) * 100,
        u=total_unconf, pct2=total_unconf / max(len(results), 1) * 100,
    ))


if __name__ == "__main__":
    main()
