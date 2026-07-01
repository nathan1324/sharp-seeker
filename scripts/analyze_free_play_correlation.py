"""Free-play (X) nightly-correlation diagnostic (read-only, streams).

The X free plays are PD totals ONLY. This isolates them from ground truth
(sent_alerts.is_free_play = 1, joined to signal_results for the graded result)
and asks why nightly results sweep (all-win / all-loss).

Two competing explanations, which this separates:
  (A) Small nightly sample -- if only 2-4 free plays post per night, "all win /
      all loss" is just what tiny samples look like, not correlation.
  (B) One-sided-slate correlation -- totals share a night's run environment. If
      the night's free plays cluster Over (or Under), a single scoring
      environment sweeps them together. The tell: nights with a heavy Over/Under
      lean sweep, and even nights with MANY plays (n>=4) still sweep.

Reports, per game-night (MST off commence_time, fallback sent_at):
  n plays, Over/Under split, W-L-P, win-fraction, units.
Then: avg plays/night, all-or-nothing vs mixed rate (overall AND for n>=4
nights), win-fraction by night-lean bucket (Over-heavy / balanced / Under-heavy),
and daily-units stdev split PRE/POST a cutoff.

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_free_play_correlation.py [days] [cutoff] [db_path]
  e.g.  ... analyze_free_play_correlation.py 45 2026-06-24
  (defaults: days=45, cutoff=2026-06-24, db=/app/data/sharp_seeker.db)
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

MST = timezone(timedelta(hours=-7))


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


def _mst_date(ts, fallback):
    for cand in (ts, fallback):
        if not cand:
            continue
        try:
            dt = datetime.fromisoformat(cand.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(MST).date().isoformat()
        except (ValueError, TypeError):
            continue
    return None


def _stats(xs):
    if not xs:
        return None
    m = sum(xs) / len(xs)
    var = sum((x - m) ** 2 for x in xs) / len(xs)
    return m, math.sqrt(var), min(xs), max(xs)


def main():
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=DAYS)).isoformat()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    sql = """
        SELECT sa.event_id, sa.alert_type, sa.market_key, sa.outcome_name,
               sa.sent_at, sa.details_json,
          (SELECT MAX(o.commence_time) FROM odds_snapshots o
             WHERE o.event_id = sa.event_id) AS commence_time,
          (SELECT sr.sport_key FROM signal_results sr
             WHERE sr.event_id = sa.event_id AND sr.signal_type = sa.alert_type
               AND sr.market_key = sa.market_key AND sr.outcome_name = sa.outcome_name
             ORDER BY sr.signal_at DESC LIMIT 1) AS sport_key,
          (SELECT sr.result FROM signal_results sr
             WHERE sr.event_id = sa.event_id AND sr.signal_type = sa.alert_type
               AND sr.market_key = sa.market_key AND sr.outcome_name = sa.outcome_name
             ORDER BY sr.signal_at DESC LIMIT 1) AS result
        FROM sent_alerts sa
        WHERE sa.is_free_play = 1
          AND sa.sent_at >= ?
        ORDER BY sa.sent_at ASC
    """
    cur = conn.execute(sql, (since,))

    # dedup one play per (event, market, outcome), keep latest sent
    seen = {}
    type_market = {}
    for row in cur:
        key = (row["event_id"], row["market_key"], row["outcome_name"])
        seen[key] = row  # later sent_at overwrites (rows come ASC)
        tm = (row["alert_type"] or "?", row["market_key"] or "?")
        type_market[tm] = type_market.get(tm, 0) + 1
    conn.close()

    if not seen:
        print("No free plays (is_free_play=1) in the window. Try more [days].")
        return

    nights = {}
    ungraded = 0
    for row in seen.values():
        day = _mst_date(row["commence_time"], row["sent_at"])
        if day is None:
            continue
        result = row["result"]
        side = (row["outcome_name"] or "").strip().lower()
        nd = nights.setdefault(day, {"n": 0, "w": 0, "l": 0, "p": 0, "u": 0.0,
                                     "over": 0, "under": 0, "ungr": 0})
        nd["n"] += 1
        if side.startswith("over"):
            nd["over"] += 1
        elif side.startswith("under"):
            nd["under"] += 1
        if result is None:
            nd["ungr"] += 1
            ungraded += 1
            continue
        nd["u"] += _units(row["details_json"], result)
        if result == "won":
            nd["w"] += 1
        elif result == "lost":
            nd["l"] += 1
        else:
            nd["p"] += 1

    print("Free-play (X) nightly-correlation diagnostic - DB: " + DB_PATH)
    print("Window: last " + str(DAYS) + " days (since " + since[:10]
          + "); source: sent_alerts.is_free_play=1; nights by game date (MST)")
    tm_str = ", ".join(t + ":" + m + "=" + str(c)
                       for (t, m), c in sorted(type_market.items()))
    print("Composition: " + tm_str)
    print("(Confirms free plays are PD totals -- the steam gate cannot touch these.)\n")

    print("Night (MST)   n   W-L-P     winfrac  O/U     units")
    total_plays = tot_w = tot_l = 0
    swept = mixed = graded_nights = 0
    big_nights = big_swept = 0
    lean_buckets = {"Over-heavy": {"w": 0, "l": 0, "nights": 0},
                    "balanced": {"w": 0, "l": 0, "nights": 0},
                    "Under-heavy": {"w": 0, "l": 0, "nights": 0}}
    daily_u = []
    for day in sorted(nights):
        nd = nights[day]
        total_plays += nd["n"]
        tot_w += nd["w"]
        tot_l += nd["l"]
        daily_u.append((day, nd["u"]))
        dec = nd["w"] + nd["l"]
        ou = str(nd["over"]) + "/" + str(nd["under"])
        if dec:
            wr = nd["w"] / dec
            graded_nights += 1
            if wr == 1.0 or wr == 0.0:
                swept += 1
            elif 0.34 <= wr <= 0.66:
                mixed += 1
            if dec >= 4:
                big_nights += 1
                if wr == 1.0 or wr == 0.0:
                    big_swept += 1
            tot_ou = nd["over"] + nd["under"]
            over_share = nd["over"] / tot_ou if tot_ou else 0.5
            bucket = ("Over-heavy" if over_share >= 0.6
                      else "Under-heavy" if over_share <= 0.4 else "balanced")
            lean_buckets[bucket]["w"] += nd["w"]
            lean_buckets[bucket]["l"] += nd["l"]
            lean_buckets[bucket]["nights"] += 1
            wr_str = "{:>4.0%}".format(wr)
        else:
            wr_str = "  - "
        ungr = "  (+" + str(nd["ungr"]) + " ungraded)" if nd["ungr"] else ""
        print("  " + day + "  " + str(nd["n"]).rjust(3) + "  "
              + (str(nd["w"]) + "-" + str(nd["l"]) + "-" + str(nd["p"])).ljust(9)
              + " " + wr_str + "   " + ou.ljust(6) + " "
              + format(nd["u"], "+.2f").rjust(7) + "u" + ungr)

    avg_n = total_plays / len(nights)
    p = (tot_w / (tot_w + tot_l)) if (tot_w + tot_l) else 0.0
    print("\nOverall free-play WR " + "{:.1%}".format(p) + " on "
          + str(tot_w + tot_l) + " decided; " + str(ungraded) + " ungraded")
    print("Avg free plays/night: " + "{:.1f}".format(avg_n))

    print("\n=== Sweep analysis ===")
    print("  All-or-nothing nights (100% or 0% won): " + str(swept) + "/"
          + str(graded_nights)
          + ("  ({:.0%})".format(swept / graded_nights) if graded_nights else ""))
    print("  Mixed nights (34-66% won):              " + str(mixed) + "/"
          + str(graded_nights)
          + ("  ({:.0%})".format(mixed / graded_nights) if graded_nights else ""))
    print("  Nights with >=4 graded plays: " + str(big_nights)
          + ", of which swept: " + str(big_swept)
          + ("  ({:.0%})".format(big_swept / big_nights) if big_nights else ""))
    if avg_n < 3:
        print("  -> AVG <3 plays/night: sweeps are largely SMALL-SAMPLE. If the")
        print("     n>=4 nights rarely sweep, correlation is NOT the main driver.")
    if big_nights and big_swept / big_nights >= 0.5:
        print("  -> Even big-n nights sweep >=50%: that is genuine ONE-SIDED-SLATE")
        print("     CORRELATION, not just small sample.")

    print("\n=== Win-rate by night lean (the common-factor test) ===")
    for name in ("Over-heavy", "balanced", "Under-heavy"):
        b = lean_buckets[name]
        dec = b["w"] + b["l"]
        wr = "{:>4.0%}".format(b["w"] / dec) if dec else "  - "
        print("  " + name.ljust(12) + " " + str(b["nights"]).rjust(2)
              + " nights   " + (str(b["w"]) + "-" + str(b["l"])).ljust(8)
              + " WR " + wr)
    print("  (If Over-heavy and Under-heavy nights swing to opposite extremes,")
    print("   the night's scoring environment is the common factor.)")

    print("\n=== Daily units volatility PRE vs POST " + CUTOFF + " ===")
    for label, xs in (("PRE  ", [u for d, u in daily_u if d < CUTOFF]),
                      ("POST ", [u for d, u in daily_u if d >= CUTOFF])):
        s = _stats(xs)
        if s is None:
            print("  " + label + "(no nights)")
            continue
        m, sd, lo, hi = s
        print("  " + label + str(len(xs)).rjust(3) + " nights   mean "
              + format(m, "+.2f").rjust(7) + "u   stdev " + "{:.2f}".format(sd).rjust(6)
              + "u   range [" + format(lo, "+.2f") + ", " + format(hi, "+.2f") + "]u")


if __name__ == "__main__":
    main()
