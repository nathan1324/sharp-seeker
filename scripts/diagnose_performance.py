"""Comprehensive performance diagnostic — grades unresolved signals, then
analyzes recent results vs historical to find regressions.

Usage:
    docker compose exec sharp-seeker python /app/scripts/diagnose_performance.py
"""

import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

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


def _wr(w, l):
    d = w + l
    if d == 0:
        return "  N/A"
    return "{p:5.1f}%".format(p=w / d * 100)


def _record(rows):
    w = sum(1 for r in rows if r["result"] == "won")
    l = sum(1 for r in rows if r["result"] == "lost")
    p = sum(1 for r in rows if r["result"] == "push")
    return w, l, p


def _print_record(label, rows, indent=0):
    w, l, p = _record(rows)
    d = w + l
    pad = " " * indent
    push_str = " ({p}P)".format(p=p) if p else ""
    print("{pad}{label:<30s} {w:>3d}-{l:<3d}{push} {wr}  n={n}".format(
        pad=pad, label=label, w=w, l=l, push=push_str,
        wr=_wr(w, l), n=d,
    ))


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


def main():
    conn = connect()
    now = datetime.now(timezone.utc)

    # ── 1. Check unresolved signals ──────────────────────────────
    unresolved = conn.execute(
        "SELECT * FROM signal_results WHERE result IS NULL ORDER BY signal_at"
    ).fetchall()

    print("=" * 65)
    print("  SHARP SEEKER — PERFORMANCE DIAGNOSTIC")
    print("  Generated: {t}".format(t=now.strftime("%Y-%m-%d %H:%M UTC")))
    print("=" * 65)
    print()

    if unresolved:
        print("!! {n} UNRESOLVED SIGNALS — need grading first".format(n=len(unresolved)))
        # Show breakdown
        by_date = defaultdict(int)
        for u in unresolved:
            d = dict(u)
            day = d["signal_at"][:10]
            by_date[day] += 1
        for day in sorted(by_date):
            print("   {day}: {n} unresolved".format(day=day, n=by_date[day]))
        print()
        print("   Run grading first:")
        print("   docker compose exec sharp-seeker python -c \"")
        print("   import asyncio; from sharp_seeker.main import create_app;")
        print("   ... or wait for next scheduled grading at 11:30 UTC")
        print()
    else:
        print("All signals resolved (grading is up to date).")
        print()

    # ── 2. Get all resolved signals ──────────────────────────────
    all_signals = conn.execute(
        "SELECT * FROM signal_results WHERE result IS NOT NULL ORDER BY signal_at"
    ).fetchall()
    all_signals = [dict(r) for r in all_signals]

    if not all_signals:
        print("No resolved signals found.")
        return

    # Parse dates
    for s in all_signals:
        try:
            s["_dt"] = datetime.fromisoformat(s["signal_at"])
            if s["_dt"].tzinfo is None:
                s["_dt"] = s["_dt"].replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            s["_dt"] = now

    first_date = min(s["_dt"] for s in all_signals)
    last_date = max(s["_dt"] for s in all_signals)
    print("Data range: {f} to {l}".format(
        f=first_date.strftime("%Y-%m-%d"), l=last_date.strftime("%Y-%m-%d"),
    ))
    print("Total resolved signals: {n}".format(n=len(all_signals)))
    print()

    # ── 3. Time periods ──────────────────────────────────────────
    cutoffs = {
        "Last 3 days": now - timedelta(days=3),
        "Last 7 days": now - timedelta(days=7),
        "Last 14 days": now - timedelta(days=14),
        "All time": first_date - timedelta(days=1),
    }

    # Hold boost deployed ~March 15
    hold_deploy = datetime(2026, 3, 15, 18, 0, tzinfo=timezone.utc)
    pre_hold = [s for s in all_signals if s["_dt"] < hold_deploy]
    post_hold = [s for s in all_signals if s["_dt"] >= hold_deploy]

    print("=" * 65)
    print("  OVERALL RECORD BY TIME PERIOD")
    print("=" * 65)
    for label, cutoff in cutoffs.items():
        period = [s for s in all_signals if s["_dt"] >= cutoff]
        _print_record(label, period)
    print()
    _print_record("Before hold boost (< Mar 15)", pre_hold)
    _print_record("After hold boost (>= Mar 15)", post_hold)
    print()

    # ── 4. Daily breakdown (last 7 days) ─────────────────────────
    print("=" * 65)
    print("  DAILY RECORD (LAST 7 DAYS)")
    print("=" * 65)
    for days_ago in range(6, -1, -1):
        day_start = (now - timedelta(days=days_ago)).replace(
            hour=0, minute=0, second=0, microsecond=0,
        )
        day_end = day_start + timedelta(days=1)
        day_sigs = [s for s in all_signals if day_start <= s["_dt"] < day_end]
        if day_sigs:
            label = day_start.strftime("%a %b %d")
            _print_record(label, day_sigs)
    print()

    # ── 5. By signal type ────────────────────────────────────────
    print("=" * 65)
    print("  BY SIGNAL TYPE — LAST 7 DAYS vs ALL TIME")
    print("=" * 65)
    week = now - timedelta(days=7)
    sig_types = sorted(set(s["signal_type"] for s in all_signals))
    for st in sig_types:
        short = SIGNAL_SHORT.get(st, st)
        all_of = [s for s in all_signals if s["signal_type"] == st]
        recent = [s for s in all_of if s["_dt"] >= week]
        w_all, l_all, _ = _record(all_of)
        w_rec, l_rec, _ = _record(recent)
        delta = ""
        if (w_all + l_all) > 0 and (w_rec + l_rec) > 0:
            all_rate = w_all / (w_all + l_all) * 100
            rec_rate = w_rec / (w_rec + l_rec) * 100
            diff = rec_rate - all_rate
            if abs(diff) >= 3:
                delta = "  ({sign}{d:.0f}pp)".format(
                    sign="+" if diff > 0 else "", d=diff,
                )
        print("  {st:<5s}  7d: {wr:>3d}-{lr:<3d} ({wpr})   All: {wa:>3d}-{la:<3d} ({wpa}){d}".format(
            st=short, wr=w_rec, lr=l_rec, wpr=_wr(w_rec, l_rec),
            wa=w_all, la=l_all, wpa=_wr(w_all, l_all), d=delta,
        ))
    print()

    # ── 6. By sport ──────────────────────────────────────────────
    print("=" * 65)
    print("  BY SPORT — LAST 7 DAYS vs ALL TIME")
    print("=" * 65)
    sports = sorted(set(s.get("sport_key", "unknown") for s in all_signals))
    for sp in sports:
        short = SPORT_SHORT.get(sp, sp)
        all_of = [s for s in all_signals if s.get("sport_key") == sp]
        recent = [s for s in all_of if s["_dt"] >= week]
        w_all, l_all, _ = _record(all_of)
        w_rec, l_rec, _ = _record(recent)
        delta = ""
        if (w_all + l_all) > 0 and (w_rec + l_rec) > 0:
            all_rate = w_all / (w_all + l_all) * 100
            rec_rate = w_rec / (w_rec + l_rec) * 100
            diff = rec_rate - all_rate
            if abs(diff) >= 3:
                delta = "  ({sign}{d:.0f}pp)".format(
                    sign="+" if diff > 0 else "", d=diff,
                )
        print("  {sp:<7s}  7d: {wr:>3d}-{lr:<3d} ({wpr})   All: {wa:>3d}-{la:<3d} ({wpa}){d}".format(
            sp=short, wr=w_rec, lr=l_rec, wpr=_wr(w_rec, l_rec),
            wa=w_all, la=l_all, wpa=_wr(w_all, l_all), d=delta,
        ))
    print()

    # ── 7. By signal type + sport (combos) ───────────────────────
    print("=" * 65)
    print("  BY TYPE:SPORT:MARKET — LAST 7 DAYS (min 3 decided)")
    print("=" * 65)
    combos = defaultdict(list)
    for s in all_signals:
        if s["_dt"] >= week:
            key = "{t}:{s}:{m}".format(
                t=SIGNAL_SHORT.get(s["signal_type"], s["signal_type"]),
                s=SPORT_SHORT.get(s.get("sport_key", "?"), s.get("sport_key", "?")),
                m=s["market_key"],
            )
            combos[key].append(s)

    combo_records = []
    for key, sigs in combos.items():
        w, l, p = _record(sigs)
        if w + l >= 3:
            combo_records.append((key, w, l, p, sigs))

    combo_records.sort(key=lambda x: x[1] / max(x[1] + x[2], 1), reverse=True)
    for key, w, l, p, sigs in combo_records:
        push_str = " ({p}P)".format(p=p) if p else ""
        marker = " ***" if w / max(w + l, 1) >= 0.60 else ""
        marker = " !!!" if w / max(w + l, 1) < 0.45 else marker
        print("  {key:<25s} {w:>3d}-{l:<3d}{push} {wr}{m}".format(
            key=key, w=w, l=l, push=push_str, wr=_wr(w, l), m=marker,
        ))
    print()

    # ── 8. Hold boost impact analysis ────────────────────────────
    print("=" * 65)
    print("  HOLD BOOST IMPACT (PD signals only)")
    print("=" * 65)
    pd_post = [s for s in post_hold if s["signal_type"] == "pinnacle_divergence"]

    if pd_post:
        # Parse details to check hold_boost
        boosted = []
        unboosted = []
        for s in pd_post:
            details = {}
            if s.get("details_json"):
                try:
                    details = json.loads(s["details_json"])
                except (json.JSONDecodeError, TypeError):
                    pass
            hb = details.get("hold_boost", 0)
            if hb and hb > 0:
                boosted.append(s)
            else:
                unboosted.append(s)

        _print_record("PD with hold boost > 0", boosted)
        _print_record("PD with no hold boost", unboosted)

        # Did any signals only fire BECAUSE of hold boost?
        # These would have base_strength that, without boost, falls below min threshold
        marginal = []
        for s in boosted:
            details = {}
            if s.get("details_json"):
                try:
                    details = json.loads(s["details_json"])
                except (json.JSONDecodeError, TypeError):
                    pass
            hb = details.get("hold_boost", 0)
            strength_without = s["signal_strength"] - hb
            # Check what threshold this signal would face
            sport = s.get("sport_key", "")
            market = s.get("market_key", "")
            # Approximate thresholds from config
            min_str = 0.25  # default for PD NHL/NBA
            if sport == "basketball_ncaab":
                min_str = 0.50
            if market == "spreads":
                min_str = 0.40
            if strength_without < min_str <= s["signal_strength"]:
                marginal.append(s)

        if marginal:
            print()
            print("  !! {n} signals ONLY fired because of hold boost:".format(
                n=len(marginal),
            ))
            _print_record("  Marginal (boost-dependent)", marginal, indent=2)
            for s in marginal:
                details = {}
                if s.get("details_json"):
                    try:
                        details = json.loads(s["details_json"])
                    except (json.JSONDecodeError, TypeError):
                        pass
                hb = details.get("hold_boost", 0)
                print("    {dt} {sp} {mk} {on} str={s:.2f} (base={b:.2f}) -> {r}".format(
                    dt=s["signal_at"][:16],
                    sp=SPORT_SHORT.get(s.get("sport_key", ""), "?"),
                    mk=s["market_key"],
                    on=s["outcome_name"][:15],
                    s=s["signal_strength"],
                    b=s["signal_strength"] - hb,
                    r=s["result"],
                ))
        else:
            print("  No marginal signals (hold boost didn't let new signals through).")
    else:
        print("  No PD signals since hold boost deployment.")
    print()

    # ── 9. Strength analysis ─────────────────────────────────────
    print("=" * 65)
    print("  WIN RATE BY STRENGTH BUCKET — LAST 7 DAYS")
    print("=" * 65)
    recent_all = [s for s in all_signals if s["_dt"] >= week]
    buckets = [
        (0.0, 0.30, "< 30%"),
        (0.30, 0.40, "30-40%"),
        (0.40, 0.50, "40-50%"),
        (0.50, 0.60, "50-60%"),
        (0.60, 0.70, "60-70%"),
        (0.70, 0.80, "70-80%"),
        (0.80, 1.01, "80%+"),
    ]
    for lo, hi, label in buckets:
        bucket = [s for s in recent_all if lo <= s["signal_strength"] < hi]
        if not bucket:
            continue
        _print_record(label, bucket, indent=2)
    print()

    # ── 10. X Free plays ─────────────────────────────────────────
    print("=" * 65)
    print("  X FREE PLAY RESULTS")
    print("=" * 65)

    free_plays = conn.execute("""
        SELECT sa.event_id, sa.alert_type, sa.market_key, sa.outcome_name,
               sa.sent_at, sr.result, sr.signal_strength, sr.sport_key,
               sr.details_json
        FROM sent_alerts sa
        LEFT JOIN signal_results sr
            ON sa.event_id = sr.event_id
            AND sa.alert_type = sr.signal_type
            AND sa.market_key = sr.market_key
            AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1
        ORDER BY sa.sent_at DESC
    """).fetchall()
    free_plays = [dict(r) for r in free_plays]

    if free_plays:
        resolved_fp = [f for f in free_plays if f["result"] is not None]
        w, l, p = _record(resolved_fp)
        unresolved_fp = [f for f in free_plays if f["result"] is None]
        print("  Overall: {w}-{l} ({wr})  {p} pushes  {u} unresolved".format(
            w=w, l=l, wr=_wr(w, l), p=p, u=len(unresolved_fp),
        ))

        # Last 10 free plays
        print()
        print("  Last 10 free plays:")
        for fp in free_plays[:10]:
            details = {}
            if fp.get("details_json"):
                try:
                    details = json.loads(fp["details_json"])
                except (json.JSONDecodeError, TypeError):
                    pass
            us_book = details.get("us_book", "")
            vb = details.get("value_books", [{}])
            book_name = vb[0].get("bookmaker", us_book) if vb else us_book
            result_str = fp["result"] or "PENDING"
            result_marker = ""
            if fp["result"] == "won":
                result_marker = " W"
            elif fp["result"] == "lost":
                result_marker = " L"
            elif fp["result"] == "push":
                result_marker = " P"

            print("    {dt} {sp:<6s} {mk:<8s} {on:<18s} {res:<8s}{m}".format(
                dt=(fp.get("sent_at") or "?")[:10],
                sp=SPORT_SHORT.get(fp.get("sport_key", ""), fp.get("sport_key", "?")),
                mk=fp.get("market_key", "?"),
                on=fp.get("outcome_name", "?")[:18],
                res=result_str,
                m=result_marker,
            ))

        # Streak
        streak_count = 0
        streak_type = None
        for fp in resolved_fp:
            if streak_type is None:
                streak_type = fp["result"]
                streak_count = 1
            elif fp["result"] == streak_type:
                streak_count += 1
            else:
                break
        if streak_type:
            print()
            print("  Current streak: {n} {t}".format(n=streak_count, t=streak_type))

        # Last 7 days
        fp_recent = [f for f in resolved_fp if f.get("sent_at") and
                     f["sent_at"] >= (now - timedelta(days=7)).isoformat()]
        if fp_recent:
            w7, l7, p7 = _record(fp_recent)
            print("  Last 7 days: {w}-{l} ({wr})".format(
                w=w7, l=l7, wr=_wr(w7, l7),
            ))
    else:
        print("  No free play data found.")
    print()

    # ── 11. Losing streak analysis ───────────────────────────────
    print("=" * 65)
    print("  STREAK ANALYSIS (ALL SIGNALS)")
    print("=" * 65)

    # Sort by date, compute current streak
    sorted_sigs = sorted(all_signals, key=lambda s: s["signal_at"], reverse=True)
    streak_count = 0
    streak_type = None
    for s in sorted_sigs:
        if s["result"] == "push":
            continue
        if streak_type is None:
            streak_type = s["result"]
            streak_count = 1
        elif s["result"] == streak_type:
            streak_count += 1
        else:
            break
    if streak_type:
        print("  Current streak: {n} {t}".format(n=streak_count, t=streak_type))

    # Find worst losing streaks
    worst_streak = 0
    current_losing = 0
    for s in sorted(all_signals, key=lambda s: s["signal_at"]):
        if s["result"] == "lost":
            current_losing += 1
            worst_streak = max(worst_streak, current_losing)
        elif s["result"] == "won":
            current_losing = 0
    print("  Worst losing streak ever: {n}".format(n=worst_streak))

    # Recent losing rate
    last_20 = sorted_sigs[:20]
    last_20_decided = [s for s in last_20 if s["result"] in ("won", "lost")]
    if last_20_decided:
        w20, l20, _ = _record(last_20_decided)
        print("  Last 20 decided signals: {w}-{l} ({wr})".format(
            w=w20, l=l20, wr=_wr(w20, l20),
        ))
    print()

    # ── 12. Recent signal details ────────────────────────────────
    print("=" * 65)
    print("  LAST 25 SIGNALS (most recent first)")
    print("=" * 65)
    for s in sorted_sigs[:25]:
        details = {}
        if s.get("details_json"):
            try:
                details = json.loads(s["details_json"])
            except (json.JSONDecodeError, TypeError):
                pass

        hb = details.get("hold_boost", "")
        hb_str = " hb={h:.2f}".format(h=hb) if hb else ""
        us_hold = details.get("us_hold")
        hold_str = " hold={h:.1f}%".format(h=us_hold * 100) if us_hold else ""

        result_marker = {
            "won": "W", "lost": "L", "push": "P",
        }.get(s["result"], "?")

        print("  {m} {dt} {type:<3s} {sp:<6s} {mk:<8s} {on:<18s} str={s:.2f}{hb}{hold}".format(
            m=result_marker,
            dt=s["signal_at"][:16],
            type=SIGNAL_SHORT.get(s["signal_type"], s["signal_type"]),
            sp=SPORT_SHORT.get(s.get("sport_key", ""), "?"),
            mk=s.get("market_key", "?"),
            on=s.get("outcome_name", "?")[:18],
            s=s["signal_strength"],
            hb=hb_str,
            hold=hold_str,
        ))
    print()

    conn.close()
    print("=" * 65)
    print("  DIAGNOSTIC COMPLETE")
    print("=" * 65)


if __name__ == "__main__":
    main()
