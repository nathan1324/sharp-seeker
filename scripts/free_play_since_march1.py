"""Free play performance since March 1st (post-algorithm tweak)."""

import json
import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

DB = "/app/data/sharp_seeker.db"
MST = ZoneInfo("America/Phoenix")
SINCE = "2026-03-01T00:00:00+00:00"

MARKET_NAMES = {"spreads": "Spread", "totals": "Total", "h2h": "Moneyline"}
RESULT_EMOJI = {"won": "W", "lost": "L", "push": "P"}


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT sa.event_id, sa.alert_type, sa.market_key, sa.outcome_name,
               sa.sent_at, sa.details_json,
               sr.result, sr.signal_strength, sr.sport_key
        FROM sent_alerts sa
        LEFT JOIN signal_results sr
          ON sa.event_id = sr.event_id
         AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key
         AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1
          AND sa.sent_at >= ?
        ORDER BY sa.sent_at ASC
    """, (SINCE,)).fetchall()

    wins = losses = pushes = pending = 0
    total = len(rows)

    print(f"=== FREE PLAYS SINCE MARCH 1 ({total} picks) ===\n")

    for r in rows:
        row = dict(r)
        result = row["result"]
        if result == "won":
            wins += 1
        elif result == "lost":
            losses += 1
        elif result == "push":
            pushes += 1
        else:
            pending += 1

        sent_utc = datetime.fromisoformat(row["sent_at"])
        if sent_utc.tzinfo is None:
            sent_utc = sent_utc.replace(tzinfo=timezone.utc)
        sent_mst = sent_utc.astimezone(MST)
        date_str = sent_mst.strftime("%b %d %I:%M%p")

        details = json.loads(row["details_json"]) if row["details_json"] else {}
        value_books = details.get("value_books", [])
        best = value_books[0] if value_books else {}
        book = best.get("bookmaker", "?").title()
        price = best.get("price")
        point = best.get("point")

        market = row["market_key"]
        market_name = MARKET_NAMES.get(market, market)

        if market == "h2h" and price is not None:
            odds_str = f"{price:+.0f}"
        elif point is not None and price is not None:
            if market == "totals":
                odds_str = f"{point} ({price:+.0f})"
            else:
                odds_str = f"{point:+.1f} ({price:+.0f})"
        elif price is not None:
            odds_str = f"{price:+.0f}"
        else:
            odds_str = "?"

        tag = RESULT_EMOJI.get(result, "PENDING")
        sport = row.get("sport_key") or "?"
        signal_type = row["alert_type"]

        print(
            f"[{tag:7s}] {date_str:16s} | {sport:25s} | {signal_type:25s} | "
            f"{market_name:10s} | {row['outcome_name']:20s} | "
            f"{odds_str:15s} @ {book:15s}"
        )

    print(f"\n=== SUMMARY (since March 1) ===")
    print(f"Record: {wins}-{losses} (W-L)")
    if pushes:
        print(f"Pushes: {pushes}")
    if pending:
        print(f"Pending: {pending}")
    resolved = wins + losses
    if resolved > 0:
        pct = wins / resolved * 100
        print(f"Win rate: {pct:.1f}% ({wins}/{resolved})")

    # Streak from March 5 onward
    print(f"\n=== STREAK (since March 5) ===")
    march5_rows = [
        dict(r) for r in rows
        if dict(r)["sent_at"] >= "2026-03-05" and dict(r)["result"] in ("won", "lost")
    ]
    m5_wins = sum(1 for r in march5_rows if r["result"] == "won")
    m5_losses = sum(1 for r in march5_rows if r["result"] == "lost")
    print(f"Record since March 5: {m5_wins}-{m5_losses}")
    results_list = [r["result"] for r in march5_rows]
    if results_list:
        current = results_list[-1]
        streak = 0
        for res in reversed(results_list):
            if res == current:
                streak += 1
            else:
                break
        print(f"Current streak: {streak} {'wins' if current == 'won' else 'losses'}")

    # By signal type since March 1
    print(f"\n=== BY SIGNAL TYPE (since March 1) ===")
    type_stats = {}
    for r in rows:
        row = dict(r)
        st = row["alert_type"]
        if st not in type_stats:
            type_stats[st] = {"won": 0, "lost": 0}
        if row["result"] in ("won", "lost"):
            type_stats[st][row["result"]] += 1

    for st, stats in sorted(type_stats.items()):
        w, l = stats["won"], stats["lost"]
        resolved = w + l
        pct = f"{w/resolved*100:.0f}%" if resolved else "N/A"
        print(f"  {st:30s} {w}-{l} ({pct})")

    # By sport since March 1
    print(f"\n=== BY SPORT (since March 1) ===")
    sport_stats = {}
    for r in rows:
        row = dict(r)
        sp = row.get("sport_key") or "unknown"
        if sp not in sport_stats:
            sport_stats[sp] = {"won": 0, "lost": 0}
        if row["result"] in ("won", "lost"):
            sport_stats[sp][row["result"]] += 1

    for sp, stats in sorted(sport_stats.items()):
        w, l = stats["won"], stats["lost"]
        resolved = w + l
        pct = f"{w/resolved*100:.0f}%" if resolved else "N/A"
        print(f"  {sp:30s} {w}-{l} ({pct})")

    conn.close()


if __name__ == "__main__":
    main()
