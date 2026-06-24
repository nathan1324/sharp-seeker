"""Confirm whether a steam_move h2h alert was posted on the WRONG (lengthening) side.

Context: 2026-06-24 Cubs ML steam alert where the sharp move was actually toward
the Mets. This script reconstructs each h2h outcome's price movement over the
steam window leading up to the alert and reports, per side:
  - direction (down = shortening = the side bettors should take)
  - which side we actually posted
  - a verdict: CORRECT side vs WRONG side (lengthening mirror shipped)

It also shows whether the correct (shortening) side existed in the data at all,
which tells us it was dropped by an upstream filter rather than never generated.

Usage (on server):
    docker compose exec sharp-seeker python /app/scripts/confirm_steam_wrong_side.py
    docker compose exec sharp-seeker python /app/scripts/confirm_steam_wrong_side.py --team Cubs --hours 18

Streams row-by-row (server has ~954MB RAM) — no fetchall on snapshots.
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone

DB_PATH_DEFAULT = "/app/data/sharp_seeker.db"
STEAM_WINDOW_MIN = 30  # matches steam_window_minutes default


def american_implied(price: float) -> float:
    """Implied win prob from American odds (for shortening/lengthening read)."""
    if price < 0:
        return (-price) / ((-price) + 100.0)
    return 100.0 / (price + 100.0)


def find_alerts(conn: sqlite3.Connection, since_iso: str, team: str | None):
    sql = (
        "SELECT id, event_id, alert_type, market_key, outcome_name, sent_at "
        "FROM sent_alerts "
        "WHERE alert_type = 'steam_move' AND market_key = 'h2h' AND sent_at >= ? "
        "ORDER BY sent_at ASC"
    )
    rows = []
    for r in conn.execute(sql, (since_iso,)):
        d = dict(r)
        if team and team.lower() not in (d["outcome_name"] or "").lower():
            continue
        rows.append(d)
    return rows


def movement_for_event(conn: sqlite3.Connection, event_id: str, sent_at: str):
    """Per (outcome, bookmaker) first vs last h2h price in the window before sent_at.

    Returns dict: outcome_name -> {"deltas": [(book, delta)], "first": p, "last": p}
    Streamed: one ORDER BY query, accumulate first/last per (outcome, book).
    """
    try:
        end = datetime.fromisoformat(sent_at)
    except ValueError:
        end = datetime.now(timezone.utc)
    start = (end - timedelta(minutes=STEAM_WINDOW_MIN)).isoformat()

    sql = (
        "SELECT outcome_name, bookmaker_key, price, fetched_at "
        "FROM odds_snapshots "
        "WHERE event_id = ? AND market_key = 'h2h' "
        "AND fetched_at >= ? AND fetched_at <= ? "
        "ORDER BY fetched_at ASC"
    )
    # outcome -> book -> {"first": price, "last": price}
    acc: dict[str, dict[str, dict]] = {}
    for r in conn.execute(sql, (event_id, start, sent_at)):
        d = dict(r)
        oc = d["outcome_name"]
        bk = d["bookmaker_key"]
        price = d["price"]
        book_map = acc.setdefault(oc, {})
        slot = book_map.get(bk)
        if slot is None:
            book_map[bk] = {"first": price, "last": price}
        else:
            slot["last"] = price

    result: dict[str, dict] = {}
    for oc, book_map in acc.items():
        deltas = []
        for bk, slot in book_map.items():
            delta = slot["last"] - slot["first"]
            if delta != 0:
                deltas.append((bk, slot["first"], slot["last"], delta))
        result[oc] = deltas
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_PATH_DEFAULT)
    ap.add_argument("--team", default="Cubs", help="substring filter on posted outcome")
    ap.add_argument("--hours", type=int, default=18, help="look back this many hours")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    since = (datetime.now(timezone.utc) - timedelta(hours=args.hours)).isoformat()
    alerts = find_alerts(conn, since, args.team)

    if not alerts:
        print("No steam_move h2h alerts matching team=" + str(args.team)
              + " in the last " + str(args.hours) + "h.")
        print("Re-run with --team '' to list ALL steam h2h alerts in the window.")
        return

    for a in alerts:
        posted = a["outcome_name"]
        print("=" * 64)
        print("ALERT  steam_move h2h  event=" + a["event_id"])
        print("  posted side : " + str(posted))
        print("  sent_at     : " + str(a["sent_at"]))

        moves = movement_for_event(conn, a["event_id"], a["sent_at"])
        if not moves:
            print("  (no h2h snapshots found in the 30-min window before sent_at)")
            continue

        bet_side = None
        for oc, deltas in moves.items():
            if not deltas:
                print("  side " + oc + ": no movement in window")
                continue
            avg = sum(x[3] for x in deltas) / len(deltas)
            direction = "down(shortening)" if avg < 0 else "up(lengthening)"
            if avg < 0:
                bet_side = oc
            ex = deltas[0]
            print("  side " + oc + ": " + str(len(deltas)) + " books moved, "
                  + "avg delta " + format(avg, "+.1f") + " -> " + direction)
            print("      e.g. " + ex[0] + " " + format(ex[1], "+.0f")
                  + " -> " + format(ex[2], "+.0f"))

        print("-" * 64)
        if bet_side is None:
            print("  VERDICT: no shortening side detected in window (inconclusive)")
        elif bet_side == posted:
            print("  VERDICT: CORRECT — posted the shortening side (" + posted + ")")
        else:
            print("  VERDICT: WRONG SIDE — sharp money was shortening "
                  + bet_side + ", but we posted " + str(posted) + ".")
            print("           The correct (" + bet_side + ") side existed in the "
                  "data but was dropped before mirror-dedup.")

    conn.close()


if __name__ == "__main__":
    main()
