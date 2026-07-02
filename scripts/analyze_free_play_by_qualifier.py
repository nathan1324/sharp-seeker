"""Free-play WR by qualifier tier + raw-combo split (read-only, streams).

Tests whether the 2026-06-16 raw-free-play-combos change (X_FREE_PLAY_RAW_COMBOS)
diluted the X feed. That change lets MLB PD signals become free plays while
BYPASSING the qualifier gate -- so 0-qualifier signals, which historically win
far less (badge data: 0q=45.7%, 1q=50.5%, 2q=57.4%, 3q=63.4%), now post to X.

If the free plays are break-even because the 0-qualifier (raw) bucket is a big,
low-WR share, this shows it directly: WR + units by qualifier_count, and a
pre/post-2026-06-16 split. Also splits by value book so the new-book hypothesis
(Hard Rock / ESPN Bet / Fanatics, added 2026-06-27) can be checked in the same
run -- expect those to have near-zero volume.

Source: sent_alerts.is_free_play=1 joined to signal_results for the graded
result; qualifier_count read from signal details. Dedup one play per
(event, market, outcome). Read-only, streams.

Usage (server):
  docker compose exec sharp-seeker python /app/scripts/analyze_free_play_by_qualifier.py [days] [cutoff] [db_path]
  e.g.  ... analyze_free_play_by_qualifier.py 45 2026-06-16
  (defaults: days=45, cutoff=2026-06-16, db=/app/data/sharp_seeker.db)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 45
CUTOFF = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "2026-06-16"
DB_PATH = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else "/app/data/sharp_seeker.db"

NEW_BOOKS = {"hardrockbet", "espnbet", "fanatics"}


def _details(*jsons):
    for j in jsons:
        if not j:
            continue
        try:
            d = json.loads(j) if isinstance(j, str) else j
            if isinstance(d, dict):
                return d
        except (json.JSONDecodeError, TypeError):
            continue
    return {}


def _units(d, result):
    if result == "push":
        return 0.0
    vb = d.get("value_books", [])
    price = vb[0].get("price") if vb else None
    if price is None:
        return 0.0
    risk = abs(price) / 100.0 if price < 0 else 100.0 / price
    return 1.0 if result == "won" else -risk


def _book(d):
    vb = d.get("value_books", [])
    if vb and vb[0].get("bookmaker"):
        return vb[0]["bookmaker"]
    return d.get("us_book") or "?"


def _new():
    return {"w": 0, "l": 0, "p": 0, "u": 0.0, "ungr": 0, "n": 0}


def _add(b, result, u):
    b["n"] += 1
    if result is None:
        b["ungr"] += 1
        return
    b["u"] += u
    if result == "won":
        b["w"] += 1
    elif result == "lost":
        b["l"] += 1
    else:
        b["p"] += 1


def _fmt(b):
    dec = b["w"] + b["l"]
    wr = "{:>5.1%}".format(b["w"] / dec) if dec else "   - "
    return ("n=" + str(b["n"]).rjust(4) + "  " + (str(b["w"]) + "-" + str(b["l"])
            + "-" + str(b["p"])).ljust(10) + " WR " + wr + "   "
            + format(b["u"], "+.2f").rjust(8) + "u"
            + ("  (" + str(b["ungr"]) + " ungr)" if b["ungr"] else ""))


def main():
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=DAYS)).isoformat()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    sql = """
        SELECT sa.event_id, sa.alert_type, sa.market_key, sa.outcome_name,
               sa.sent_at, sa.details_json AS sa_details,
          (SELECT sr.result FROM signal_results sr
             WHERE sr.event_id = sa.event_id AND sr.signal_type = sa.alert_type
               AND sr.market_key = sa.market_key AND sr.outcome_name = sa.outcome_name
             ORDER BY sr.signal_at DESC LIMIT 1) AS result,
          (SELECT sr.details_json FROM signal_results sr
             WHERE sr.event_id = sa.event_id AND sr.signal_type = sa.alert_type
               AND sr.market_key = sa.market_key AND sr.outcome_name = sa.outcome_name
             ORDER BY sr.signal_at DESC LIMIT 1) AS sr_details,
          (SELECT sr.sport_key FROM signal_results sr
             WHERE sr.event_id = sa.event_id AND sr.signal_type = sa.alert_type
               AND sr.market_key = sa.market_key AND sr.outcome_name = sa.outcome_name
             ORDER BY sr.signal_at DESC LIMIT 1) AS sport_key
        FROM sent_alerts sa
        WHERE sa.is_free_play = 1 AND sa.sent_at >= ?
        ORDER BY sa.sent_at ASC
    """
    cur = conn.execute(sql, (since,))
    seen = {}
    for row in cur:
        seen[(row["event_id"], row["market_key"], row["outcome_name"])] = row
    conn.close()

    if not seen:
        print("No free plays (is_free_play=1) in the window.")
        return

    by_q = {}
    by_book = {}
    by_sport_q = {}
    pre = _new()
    post = _new()
    overall = _new()
    for row in seen.values():
        d = _details(row["sr_details"], row["sa_details"])
        q = d.get("qualifier_count")
        qk = q if isinstance(q, int) else -1  # -1 = unknown
        result = row["result"]
        u = _units(d, result)
        _add(by_q.setdefault(qk, _new()), result, u)
        _add(by_book.setdefault(_book(d), _new()), result, u)
        sp = (row["sport_key"] or "?")
        _add(by_sport_q.setdefault((sp, "raw(0q)" if qk == 0 else "gated(1+)" if qk >= 1 else "unknown"), _new()), result, u)
        _add(overall, result, u)
        _add(pre if row["sent_at"][:10] < CUTOFF else post, result, u)

    print("Free-play WR by qualifier tier - DB: " + DB_PATH)
    print("Window: last " + str(DAYS) + " days (since " + since[:10]
          + "); source sent_alerts.is_free_play=1; dedup per event/market/side\n")
    print("OVERALL   " + _fmt(overall))

    print("\n=== By qualifier tier (0q historically ~45.7% WR) ===")
    for qk in sorted(by_q):
        label = ("q=" + str(qk)) if qk >= 0 else "q=unknown"
        tag = "  <- RAW bypass (0-qualifier)" if qk == 0 else ""
        print("  " + label.ljust(11) + _fmt(by_q[qk]) + tag)

    raw = by_q.get(0)
    gated = _new()
    for qk, b in by_q.items():
        if qk >= 1:
            for k in ("w", "l", "p", "u", "ungr", "n"):
                gated[k] += b[k]
    if raw:
        print("\n  RAW (0q):   " + _fmt(raw))
    print("  GATED (1+):" + _fmt(gated))

    print("\n=== By value book (new books added 2026-06-27) ===")
    for bk, b in sorted(by_book.items(), key=lambda kv: kv[1]["n"], reverse=True):
        tag = "  <- NEW BOOK" if bk in NEW_BOOKS else ""
        print("  " + bk.ljust(15) + _fmt(b) + tag)

    print("\n=== By sport x tier ===")
    for (sp, tier), b in sorted(by_sport_q.items(), key=lambda kv: kv[1]["n"], reverse=True):
        print("  " + (sp + " / " + tier).ljust(30) + _fmt(b))

    print("\n=== PRE vs POST " + CUTOFF + " (raw-combo enable date) ===")
    print("  PRE  " + _fmt(pre))
    print("  POST " + _fmt(post))
    print("  If POST volume jumped and WR fell vs PRE, the raw-combo bypass")
    print("  (not the steam fix or the new books) reshaped the feed.")


if __name__ == "__main__":
    main()
