"""Diagnose free play performance — breakdown by week, sport, signal type, qualifier tags."""

import json
import sqlite3
from collections import defaultdict
from datetime import datetime

DB = "/app/data/sharp_seeker.db"


def run():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    # Get all free plays with their results
    rows = conn.execute("""
        SELECT sa.event_id, sa.market_key, sa.outcome_name, sa.sent_at,
               sa.details_json, sa.alert_type,
               sr.result, sr.signal_strength
        FROM sent_alerts sa
        LEFT JOIN signal_results sr
          ON sa.event_id = sr.event_id
         AND sa.alert_type = sr.signal_type
         AND sa.market_key = sr.market_key
         AND sa.outcome_name = sr.outcome_name
        WHERE sa.is_free_play = 1
        ORDER BY sa.sent_at ASC
    """).fetchall()

    if not rows:
        print("No free plays found.")
        return

    print(f"=== FREE PLAY DIAGNOSIS ({len(rows)} total) ===\n")

    # --- Overall record ---
    won = sum(1 for r in rows if r["result"] == "won")
    lost = sum(1 for r in rows if r["result"] == "lost")
    push = sum(1 for r in rows if r["result"] == "push")
    pending = sum(1 for r in rows if r["result"] is None)
    decided = won + lost
    rate = f"{won/decided:.1%}" if decided else "N/A"
    print(f"Overall: {rate} ({won}W / {lost}L / {push}P / {pending} pending)\n")

    # --- By week ---
    print("=== BY WEEK ===")
    weekly = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0, "pending": 0})
    for r in rows:
        dt = datetime.fromisoformat(r["sent_at"].replace("Z", "+00:00"))
        week = dt.strftime("%Y-W%U")
        res = r["result"] or "pending"
        weekly[week][res] += 1

    for week in sorted(weekly.keys()):
        c = weekly[week]
        d = c["won"] + c["lost"]
        wr = f"{c['won']/d:.0%}" if d else "N/A"
        print(f"  {week}: {wr} ({c['won']}W/{c['lost']}L/{c['push']}P/{c['pending']}pend)")

    # --- By signal type ---
    print("\n=== BY SIGNAL TYPE ===")
    by_type = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for r in rows:
        if r["result"] in ("won", "lost", "push"):
            by_type[r["alert_type"]][r["result"]] += 1
    for st, c in sorted(by_type.items()):
        d = c["won"] + c["lost"]
        wr = f"{c['won']/d:.0%}" if d else "N/A"
        print(f"  {st}: {wr} ({c['won']}W/{c['lost']}L/{c['push']}P)")

    # --- By sport ---
    print("\n=== BY SPORT ===")
    by_sport = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for r in rows:
        if r["result"] not in ("won", "lost", "push"):
            continue
        details = {}
        if r["details_json"]:
            try:
                details = json.loads(r["details_json"])
            except (json.JSONDecodeError, TypeError):
                pass
        sport = details.get("sport_key", "unknown")
        by_sport[sport][r["result"]] += 1
    for sp, c in sorted(by_sport.items()):
        d = c["won"] + c["lost"]
        wr = f"{c['won']/d:.0%}" if d else "N/A"
        print(f"  {sp}: {wr} ({c['won']}W/{c['lost']}L/{c['push']}P)")

    # --- By market ---
    print("\n=== BY MARKET ===")
    by_market = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for r in rows:
        if r["result"] in ("won", "lost", "push"):
            by_market[r["market_key"]][r["result"]] += 1
    for mk, c in sorted(by_market.items()):
        d = c["won"] + c["lost"]
        wr = f"{c['won']/d:.0%}" if d else "N/A"
        print(f"  {mk}: {wr} ({c['won']}W/{c['lost']}L/{c['push']}P)")

    # --- By qualifier tags ---
    print("\n=== BY QUALIFIER TAGS ===")
    by_tags = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for r in rows:
        if r["result"] not in ("won", "lost", "push"):
            continue
        details = {}
        if r["details_json"]:
            try:
                details = json.loads(r["details_json"])
            except (json.JSONDecodeError, TypeError):
                pass
        tags = details.get("qualifier_tags", [])
        tag_key = " + ".join(sorted(tags)) if tags else "(none)"
        by_tags[tag_key][r["result"]] += 1
    for tags, c in sorted(by_tags.items()):
        d = c["won"] + c["lost"]
        wr = f"{c['won']/d:.0%}" if d else "N/A"
        print(f"  {tags}: {wr} ({c['won']}W/{c['lost']}L/{c['push']}P)")

    # --- By signal_type:sport:market combo ---
    print("\n=== BY TYPE:SPORT:MARKET COMBO ===")
    by_combo = defaultdict(lambda: {"won": 0, "lost": 0, "push": 0})
    for r in rows:
        if r["result"] not in ("won", "lost", "push"):
            continue
        details = {}
        if r["details_json"]:
            try:
                details = json.loads(r["details_json"])
            except (json.JSONDecodeError, TypeError):
                pass
        sport = details.get("sport_key", "unknown")
        combo = f"{r['alert_type']}:{sport}:{r['market_key']}"
        by_combo[combo][r["result"]] += 1
    for combo, c in sorted(by_combo.items(), key=lambda x: -(x[1]["won"]+x[1]["lost"])):
        d = c["won"] + c["lost"]
        wr = f"{c['won']/d:.0%}" if d else "N/A"
        print(f"  {combo}: {wr} ({c['won']}W/{c['lost']}L/{c['push']}P)")

    # --- Recent 4 weeks: individual results ---
    print("\n=== LAST 4 WEEKS — INDIVIDUAL FREE PLAYS ===")
    recent = [r for r in rows if r["result"] in ("won", "lost", "push")][-60:]
    for r in recent:
        details = {}
        if r["details_json"]:
            try:
                details = json.loads(r["details_json"])
            except (json.JSONDecodeError, TypeError):
                pass
        sport = details.get("sport_key", "unknown")
        tags = details.get("qualifier_tags", [])
        tag_str = "+".join(t[:5] for t in tags)
        emoji = {"won": "W", "lost": "L", "push": "P"}.get(r["result"], "?")
        strength = r["signal_strength"] or 0
        dt = r["sent_at"][:16]
        print(f"  {emoji} {dt} {r['alert_type'][:8]:8s} {sport:25s} {r['market_key']:8s} {r['outcome_name']:20s} str={strength:.2f} [{tag_str}]")

    # --- Cross-book hold distribution for wins vs losses ---
    print("\n=== CROSS-BOOK HOLD: WINS vs LOSSES ===")
    win_holds = []
    loss_holds = []
    for r in rows:
        if r["result"] not in ("won", "lost"):
            continue
        details = {}
        if r["details_json"]:
            try:
                details = json.loads(r["details_json"])
            except (json.JSONDecodeError, TypeError):
                pass
        cbh = details.get("cross_book_hold")
        if cbh is not None:
            if r["result"] == "won":
                win_holds.append(cbh)
            else:
                loss_holds.append(cbh)
    if win_holds:
        print(f"  Wins  — avg hold: {sum(win_holds)/len(win_holds):.3f}, median: {sorted(win_holds)[len(win_holds)//2]:.3f}, n={len(win_holds)}")
    if loss_holds:
        print(f"  Losses — avg hold: {sum(loss_holds)/len(loss_holds):.3f}, median: {sorted(loss_holds)[len(loss_holds)//2]:.3f}, n={len(loss_holds)}")

    conn.close()


if __name__ == "__main__":
    run()
