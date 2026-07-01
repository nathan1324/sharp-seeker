"""Daily-correlation / volatility diagnostic (read-only, streams).

Answers "why are results swingy — win big or lose big, all right or all wrong?"
That pattern is the signature of CORRELATED picks: bets that share a common
factor (a slate's league-wide scoring environment) resolve together instead of
averaging out. This script quantifies the correlation four ways:

  1. Daily win-fraction histogram. Independent picks at win-rate p give a bell
     around p. Correlated picks give a U-shape (days pile up near 0% and 100%).
     The "sweep share" = fraction of days that were >=80% or <=20% won.
  2. Directional lean per slate. For totals, Over vs Under share per day. A
     one-sided slate (mostly Over or mostly Under) is what makes a whole night
     cash or bust together.
  3. Multi-leg-per-game exposure. plays / distinct-events per day. >1 means the
     same game is posted as several correlated legs (e.g. Over 8.5 AND Over 9.0),
     which inflates both the up days and the down days.
  4. Daily units mean/stdev, split PRE vs POST a cutoff date, to show whether
     volatility actually increased around a change.

Sent-to-Discord only, dedup latest fire per (event, market, outcome). Days are
bucketed in MST (America/Phoenix, UTC-7, no DST) off signal_at.

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_daily_correlation.py [days] [cutoff_YYYY-MM-DD] [db_path] [all]
  e.g.  ... analyze_daily_correlation.py 45 2026-06-24
  (defaults: days=45, cutoff=2026-06-24, db=/app/data/sharp_seeker.db, sent-only)
"""

from __future__ import annotations

import json
import math
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 45
CUTOFF = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "2026-06-24"
DB_PATH = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else "/app/data/sharp_seeker.db"
SENT_ONLY = "all" not in sys.argv[1:]

MST = timezone(timedelta(hours=-7))  # America/Phoenix, no DST


def _units(details_json, result):
    if result == "push":
        return 0.0
    price = None
    if details_json:
        try:
            d = json.loads(details_json) if isinstance(details_json, str) else details_json
            vb = d.get("value_books", [])
            if vb:
                price = vb[0].get("price")
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass
    if price is None:
        return 0.0
    risk = abs(price) / 100.0 if price < 0 else 100.0 / price
    return 1.0 if result == "won" else -risk


def _new_day():
    return {"n": 0, "w": 0, "l": 0, "p": 0, "u": 0.0,
            "over": 0, "under": 0, "events": set()}


def _wr(w, l):
    dec = w + l
    return (w / dec) if dec else None


def _bar(frac, width=30):
    filled = int(round(frac * width))
    return "#" * filled + "." * (width - filled)


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
            " AND sa.alert_type = signal_results.signal_type"
            " AND sa.market_key = signal_results.market_key"
            " AND sa.outcome_name = signal_results.outcome_name)"
        )
    sql = """
        SELECT signal_at, result, details_json, sport_key, market_key,
               outcome_name, event_id
        FROM (
            SELECT signal_at, result, details_json, sport_key, market_key,
                   outcome_name, event_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id, market_key, outcome_name
                       ORDER BY signal_at DESC
                   ) AS rn
            FROM signal_results
            WHERE result IS NOT NULL
              AND signal_at >= ?{sent}
        )
        WHERE rn = 1
        ORDER BY signal_at ASC
    """.format(sent=sent_clause)
    cur = conn.execute(sql, (since,))

    days = {}          # mst_date -> day bucket
    per_market = {}    # (sport, market) -> {"n","over","under"} for lean
    for row in cur:
        result = row["result"]
        try:
            dt = datetime.fromisoformat(row["signal_at"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            continue
        day_key = dt.astimezone(MST).date().isoformat()
        d = days.setdefault(day_key, _new_day())
        u = _units(row["details_json"], result)
        d["n"] += 1
        d["u"] += u
        d["events"].add(row["event_id"])
        if result == "won":
            d["w"] += 1
        elif result == "lost":
            d["l"] += 1
        else:
            d["p"] += 1
        side = (row["outcome_name"] or "").strip().lower()
        mk = (row["market_key"] or "").lower()
        is_over = side.startswith("over")
        is_under = side.startswith("under")
        if mk == "totals" and (is_over or is_under):
            if is_over:
                d["over"] += 1
            else:
                d["under"] += 1
            pm = per_market.setdefault((row["sport_key"] or "?", mk),
                                       {"n": 0, "over": 0, "under": 0})
            pm["n"] += 1
            pm["over"] += 1 if is_over else 0
            pm["under"] += 1 if is_under else 0

    conn.close()

    if not days:
        print("No sent+graded plays in the window. Try a larger [days] or 'all'.")
        return

    scope = "SENT to Discord only" if SENT_ONLY else "ALL recorded (incl. unsent)"
    print("Daily-correlation diagnostic - DB: " + DB_PATH)
    print("Window: last " + str(DAYS) + " days (since " + since[:10]
          + "); " + scope + "; dedup latest fire; days in MST\n")

    # ---- per-day table + histogram accumulation --------------------------
    ordered = sorted(days.items())
    sweep_days = 0
    mixed_days = 0
    graded_days = 0
    hist = {"0-20": 0, "20-40": 0, "40-60": 0, "60-80": 0, "80-100": 0}
    all_units = []
    total_plays = 0
    total_events = 0

    print("Date (MST)   plays  evts  W-L-P     winfrac  O/U      units")
    for day, d in ordered:
        total_plays += d["n"]
        total_events += len(d["events"])
        all_units.append(d["u"])
        wr = _wr(d["w"], d["l"])
        ou = str(d["over"]) + "/" + str(d["under"])
        wr_str = "  -  " if wr is None else "{:>4.0%}".format(wr)
        print("  " + day + "   " + str(d["n"]).rjust(4) + "  "
              + str(len(d["events"])).rjust(4) + "  "
              + (str(d["w"]) + "-" + str(d["l"]) + "-" + str(d["p"])).ljust(9)
              + " " + wr_str + "   " + ou.ljust(7)
              + " " + format(d["u"], "+.2f").rjust(8) + "u")
        if wr is not None:
            graded_days += 1
            if wr >= 0.8 or wr <= 0.2:
                sweep_days += 1
            if 0.4 <= wr <= 0.6:
                mixed_days += 1
            if wr < 0.2:
                hist["0-20"] += 1
            elif wr < 0.4:
                hist["20-40"] += 1
            elif wr < 0.6:
                hist["40-60"] += 1
            elif wr < 0.8:
                hist["60-80"] += 1
            else:
                hist["80-100"] += 1

    # ---- overall win rate + independent baseline -------------------------
    tot_w = sum(d["w"] for _, d in ordered)
    tot_l = sum(d["l"] for _, d in ordered)
    p = _wr(tot_w, tot_l) or 0.0
    avg_plays_day = total_plays / len(ordered)

    print("\n=== 1) Daily win-fraction distribution (U-shape = correlated) ===")
    print("Overall WR " + "{:.1%}".format(p)
          + " on " + str(tot_w + tot_l) + " decided; "
          + "{:.1f}".format(avg_plays_day) + " plays/day\n")
    order = ["0-20", "20-40", "40-60", "60-80", "80-100"]
    for b in order:
        c = hist[b]
        frac = c / graded_days if graded_days else 0.0
        print("  " + b.rjust(6) + "%  " + str(c).rjust(3) + " days  "
              + _bar(frac))
    print("\n  Sweep days (>=80% or <=20% won): " + str(sweep_days) + "/"
          + str(graded_days)
          + ("  ({:.0%})".format(sweep_days / graded_days) if graded_days else ""))
    print("  Mixed days (40-60% won):         " + str(mixed_days) + "/"
          + str(graded_days)
          + ("  ({:.0%})".format(mixed_days / graded_days) if graded_days else ""))
    print("  If picks were INDEPENDENT at " + "{:.0%}".format(p)
          + " and ~" + "{:.0f}".format(avg_plays_day)
          + " plays/day, most days would land in the 40-60% band.")
    print("  A U-shape (piles at the ends) = picks moving together.")

    # ---- 2) directional lean ---------------------------------------------
    print("\n=== 2) Directional lean on totals (one-sided slate = swing risk) ===")
    if per_market:
        for (sport, mk), pm in sorted(per_market.items(),
                                      key=lambda kv: kv[1]["n"], reverse=True):
            n = pm["n"]
            over_share = pm["over"] / n if n else 0.0
            print("  " + (sport + ":" + mk).ljust(28) + " n=" + str(n).rjust(4)
                  + "  Over " + "{:>4.0%}".format(over_share)
                  + " / Under " + "{:>4.0%}".format(1 - over_share)
                  + "   " + _bar(over_share, 20))
        print("  (50/50 is balanced; a heavy lean means the book systematically")
        print("   picks one side, so one scoring environment sweeps the slate.)")
    else:
        print("  No totals plays in window.")

    # ---- 3) multi-leg per game -------------------------------------------
    print("\n=== 3) Multi-leg-per-game exposure ===")
    ratio = total_plays / total_events if total_events else 0.0
    print("  " + str(total_plays) + " graded plays across " + str(total_events)
          + " distinct games = " + "{:.2f}".format(ratio) + " legs/game")
    if ratio > 1.15:
        print("  >1 means the SAME game ships as multiple correlated legs")
        print("  (e.g. Over 8.5 AND Over 9.0) — inflates both up and down days.")
    else:
        print("  ~1.0 - one leg per game, no correlated stacking here.")

    # ---- 4) volatility pre/post cutoff -----------------------------------
    print("\n=== 4) Daily units volatility: PRE vs POST " + CUTOFF + " ===")
    pre = [u for (day, d), u in zip(ordered, all_units) if day < CUTOFF]
    post = [u for (day, d), u in zip(ordered, all_units) if day >= CUTOFF]

    def _stats(xs):
        if not xs:
            return None
        m = sum(xs) / len(xs)
        var = sum((x - m) ** 2 for x in xs) / len(xs)
        return m, math.sqrt(var), min(xs), max(xs), len(xs)

    for label, xs in (("PRE  ", pre), ("POST ", post)):
        s = _stats(xs)
        if s is None:
            print("  " + label + " (no days)")
            continue
        m, sd, lo, hi, k = s
        print("  " + label + str(k).rjust(3) + " days   mean "
              + format(m, "+.2f").rjust(7) + "u/day   stdev "
              + "{:.2f}".format(sd).rjust(6) + "u   range ["
              + format(lo, "+.2f") + ", " + format(hi, "+.2f") + "]u")
    print("  Rising stdev = bets got more correlated / exposure more concentrated.")


if __name__ == "__main__":
    main()
