"""Steam side-selection / nightly-correlation diagnostic (read-only, streams).

Tests the hypothesis: the 2026-06-24 steam wrong-side gate (emit only the
shortening/steam side) changed the play population in a way that makes nightly
results sweep (all-win or all-loss).

The gate did NOT change which side is "correct" (it mirrors _pick_best_signal).
What it changed: it stopped shipping the CONTRARIAN mirror side that used to leak
through when the correct side was filtered before dedup. Those contrarian plays
diversified the slate. This script measures whether that diversification really
vanished after the fix, and whether nightly correlation rose because of it --
vs. the boring alternative that per-night volume simply got too small for a
mixed result to be possible.

Per PRE/POST window (split at the cutoff) it reports:
  * plays, and CONTRARIAN share -- a play whose side is AGAINST the movement
    (details.direction): over+down / under+up for totals, "up" for h2h/spreads.
    Expect POST ~= 0% (the gate's whole job). A meaningful PRE share that
    disappears POST = the hedge the fix removed.
  * per-night side lean (Over% vs Under% for totals) -- one-sidedness = the
    common factor that lets a night sweep.
  * all-or-nothing nights (100% or 0% won) vs mixed nights, and avg plays/night
    so you can tell correlation from small-sample.
  * daily-units stdev PRE vs POST.

Sent-to-Discord only, dedup latest fire per (event, market, outcome), nights in
MST off signal_at. Defaults to signal_type=steam_move.

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_steam_side_correlation.py [days] [cutoff] [signal_type] [db_path] [all]
  e.g.  ... analyze_steam_side_correlation.py 45 2026-06-24
  (defaults: days=45, cutoff=2026-06-24, type=steam_move, db=/app/data/sharp_seeker.db, sent-only)
"""

from __future__ import annotations

import json
import math
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 45
CUTOFF = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "2026-06-24"
SIGNAL_TYPE = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else "steam_move"
DB_PATH = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] else "/app/data/sharp_seeker.db"
SENT_ONLY = "all" not in sys.argv[1:]

MST = timezone(timedelta(hours=-7))


def _parse_details(details_json):
    if not details_json:
        return {}
    try:
        d = json.loads(details_json) if isinstance(details_json, str) else details_json
        return d if isinstance(d, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _units(d, result):
    if result == "push":
        return 0.0
    price = None
    vb = d.get("value_books", [])
    if vb:
        price = vb[0].get("price")
    if price is None:
        return 0.0
    risk = abs(price) / 100.0 if price < 0 else 100.0 / price
    return 1.0 if result == "won" else -risk


def _is_contrarian(market_key, outcome_name, direction):
    """True if the played side is AGAINST the movement (the fix should remove these)."""
    side = (outcome_name or "").strip().lower()
    if market_key == "totals":
        with_steam = (side.startswith("over") and direction == "up") or (
            side.startswith("under") and direction == "down")
        return not with_steam
    # h2h / spreads: the steam (shortening) side is "down"
    return direction != "down"


def _stats(xs):
    if not xs:
        return None
    m = sum(xs) / len(xs)
    var = sum((x - m) ** 2 for x in xs) / len(xs)
    return m, math.sqrt(var), min(xs), max(xs), len(xs)


def _report(label, rows):
    """rows: list of dicts with keys day, result, u, market_key, outcome_name, direction, contrarian, over, under."""
    if not rows:
        print("\n===== " + label + ": no plays =====")
        return
    nights = {}
    contrarian = 0
    over = under = 0
    for r in rows:
        if r["contrarian"]:
            contrarian += 1
        over += r["over"]
        under += r["under"]
        nd = nights.setdefault(r["day"], {"w": 0, "l": 0, "p": 0, "u": 0.0,
                                          "over": 0, "under": 0, "n": 0})
        nd["n"] += 1
        nd["u"] += r["u"]
        nd["over"] += r["over"]
        nd["under"] += r["under"]
        if r["result"] == "won":
            nd["w"] += 1
        elif r["result"] == "lost":
            nd["l"] += 1
        else:
            nd["p"] += 1

    n_plays = len(rows)
    tot_ou = over + under
    print("\n===== " + label + " (" + str(len(nights)) + " nights, "
          + str(n_plays) + " plays) =====")
    print("  Contrarian (against-movement) plays: " + str(contrarian) + "/"
          + str(n_plays)
          + ("  ({:.0%})".format(contrarian / n_plays) if n_plays else "")
          + "   <- the fix should drive this to ~0")
    if tot_ou:
        print("  Totals side split: Over " + "{:.0%}".format(over / tot_ou)
              + " / Under " + "{:.0%}".format(under / tot_ou)
              + "  (n=" + str(tot_ou) + ")")

    # nightly sweep analysis
    swept = mixed = graded_nights = 0
    daily_u = []
    print("\n  Night (MST)   n   W-L-P    winfrac  O/U")
    for day in sorted(nights):
        nd = nights[day]
        daily_u.append(nd["u"])
        dec = nd["w"] + nd["l"]
        if dec:
            graded_nights += 1
            wr = nd["w"] / dec
            if wr == 1.0 or wr == 0.0:
                swept += 1
            elif 0.34 <= wr <= 0.66:
                mixed += 1
            wr_str = "{:>4.0%}".format(wr)
        else:
            wr_str = "  - "
        print("    " + day + "  " + str(nd["n"]).rjust(3) + "  "
              + (str(nd["w"]) + "-" + str(nd["l"]) + "-" + str(nd["p"])).ljust(8)
              + " " + wr_str + "   " + (str(nd["over"]) + "/" + str(nd["under"])))

    avg_n = n_plays / len(nights) if nights else 0.0
    print("\n  Avg plays/night: " + "{:.1f}".format(avg_n))
    print("  All-or-nothing nights (100% or 0% won): " + str(swept) + "/"
          + str(graded_nights)
          + ("  ({:.0%})".format(swept / graded_nights) if graded_nights else ""))
    print("  Mixed nights (34-66% won):              " + str(mixed) + "/"
          + str(graded_nights)
          + ("  ({:.0%})".format(mixed / graded_nights) if graded_nights else ""))
    s = _stats(daily_u)
    if s:
        m, sd, lo, hi, k = s
        print("  Daily units: mean " + format(m, "+.2f") + "u  stdev "
              + "{:.2f}".format(sd) + "u  range [" + format(lo, "+.2f")
              + ", " + format(hi, "+.2f") + "]u")
    if avg_n < 2.5 and graded_nights:
        print("  NOTE: <2.5 plays/night -- 'all win/all loss' may just be small")
        print("        sample, not correlation. Weigh against the PRE window.")


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
        SELECT signal_at, result, details_json, market_key, outcome_name, event_id
        FROM (
            SELECT signal_at, result, details_json, market_key, outcome_name, event_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id, market_key, outcome_name
                       ORDER BY signal_at DESC
                   ) AS rn
            FROM signal_results
            WHERE result IS NOT NULL
              AND signal_type = ?
              AND signal_at >= ?{sent}
        )
        WHERE rn = 1
        ORDER BY signal_at ASC
    """.format(sent=sent_clause)
    cur = conn.execute(sql, (SIGNAL_TYPE, since))

    pre, post = [], []
    for row in cur:
        d = _parse_details(row["details_json"])
        direction = d.get("direction", "")
        try:
            dt = datetime.fromisoformat(row["signal_at"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            continue
        day = dt.astimezone(MST).date().isoformat()
        mk = (row["market_key"] or "").lower()
        side = (row["outcome_name"] or "").strip().lower()
        rec = {
            "day": day,
            "result": row["result"],
            "u": _units(d, row["result"]),
            "market_key": mk,
            "outcome_name": row["outcome_name"],
            "direction": direction,
            "contrarian": _is_contrarian(mk, row["outcome_name"], direction),
            "over": 1 if (mk == "totals" and side.startswith("over")) else 0,
            "under": 1 if (mk == "totals" and side.startswith("under")) else 0,
        }
        (pre if day < CUTOFF else post).append(rec)

    conn.close()

    scope = "SENT to Discord only" if SENT_ONLY else "ALL recorded"
    print(SIGNAL_TYPE + " side-selection / nightly-correlation diagnostic")
    print("DB: " + DB_PATH)
    print("Window: last " + str(DAYS) + " days (since " + since[:10] + "); "
          + scope + "; dedup latest fire; nights in MST; cutoff " + CUTOFF)
    print("Interpretation: if PRE has contrarian plays that POST does not, the")
    print("fix removed a hedge -> expect POST to be more one-sided per night and")
    print("sweep more often. If instead POST just has far fewer plays/night, the")
    print("swings are small-sample, not correlation.")

    _report("PRE  " + CUTOFF, pre)
    _report("POST " + CUTOFF, post)


if __name__ == "__main__":
    main()
