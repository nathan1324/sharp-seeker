"""Audit all resolved spread/total signals for grading errors.

Uses math to determine which grades could have flipped without needing
game scores.  For spreads, game margins are integers and lines are
typically x.5, so we can reason about whether the grade could change.

Categories:
  - SAFE:  the correct line is equal or more favorable → grade can't change
  - AT RISK: the correct line is less favorable → grade MIGHT have flipped
  - PUSH: graded as push with mismatched lines → definitely needs review

For "lost" signals: if the correct (details_json) point is MORE favorable
than the Pinnacle point used, the grade might flip to "won".

For "won" signals: if the correct point is LESS favorable, the grade
might flip to "lost".
"""

import asyncio
import json
from collections import defaultdict

import aiosqlite


def could_flip(market_key: str, outcome_name: str, current_result: str,
               pinnacle_point: float, details_point: float) -> bool:
    """Determine if using details_point instead of pinnacle_point could
    change the grade, without knowing the actual game score.

    Returns True if the grade MIGHT change, False if it definitely can't.
    """
    if current_result == "push":
        # Push means margin + pinnacle_point == 0 exactly.
        # With details_point: margin + details_point = details_point - pinnacle_point
        # This is nonzero when the points differ, so the grade WILL change.
        return True

    if market_key == "spreads":
        # For spreads: higher point is more favorable for the bettor.
        # "won" means margin + point > 0.  A lower correct point makes it
        # harder to win → could flip.
        # "lost" means margin + point < 0.  A higher correct point makes it
        # easier to win → could flip.
        if current_result == "won":
            return details_point < pinnacle_point
        elif current_result == "lost":
            return details_point > pinnacle_point

    elif market_key == "totals":
        # For totals: "Over" wins when combined > point (lower point = easier).
        # "Under" wins when combined < point (higher point = easier).
        if outcome_name == "Over":
            if current_result == "won":
                return details_point > pinnacle_point  # higher line, harder to go over
            elif current_result == "lost":
                return details_point < pinnacle_point  # lower line, easier to go over
        elif outcome_name == "Under":
            if current_result == "won":
                return details_point < pinnacle_point  # lower line, harder to stay under
            elif current_result == "lost":
                return details_point > pinnacle_point  # higher line, easier to stay under

    return True  # unknown — flag for review


async def main() -> None:
    db = await aiosqlite.connect("/app/data/sharp_seeker.db")
    db.row_factory = aiosqlite.Row

    cursor = await db.execute("""
        SELECT * FROM signal_results
        WHERE result IS NOT NULL
          AND market_key IN ('spreads', 'totals')
          AND details_json IS NOT NULL
        ORDER BY signal_at
    """)
    rows = await cursor.fetchall()

    print(f"Checking {len(rows)} resolved spread/total signals with details_json...\n")

    safe = 0
    at_risk = []
    stats = defaultdict(int)  # "lost→won?", "won→lost?", "push→?"

    for row in rows:
        sig = dict(row)
        event_id = sig["event_id"]
        market_key = sig["market_key"]
        outcome_name = sig["outcome_name"]
        signal_at = sig["signal_at"]
        current_result = sig["result"]

        try:
            details = json.loads(sig["details_json"])
            value_books = details.get("value_books", [])
            if not value_books or value_books[0].get("point") is None:
                safe += 1
                continue
            details_point = float(value_books[0]["point"])
        except (json.JSONDecodeError, TypeError, ValueError):
            safe += 1
            continue

        ref_cursor = await db.execute("""
            SELECT point FROM odds_snapshots
            WHERE event_id = ? AND market_key = ? AND outcome_name = ?
              AND fetched_at <= ? AND point IS NOT NULL
              AND bookmaker_key = 'pinnacle'
            ORDER BY fetched_at DESC
            LIMIT 1
        """, (event_id, market_key, outcome_name, signal_at))
        ref_row = await ref_cursor.fetchone()

        if not ref_row:
            safe += 1
            continue

        pinnacle_point = float(ref_row["point"])

        if details_point == pinnacle_point:
            safe += 1
            continue

        if not could_flip(market_key, outcome_name, current_result,
                          pinnacle_point, details_point):
            safe += 1
            continue

        # This signal's grade MIGHT be wrong
        delta = abs(details_point - pinnacle_point)
        direction = f"{current_result}→?"

        if current_result == "lost":
            direction = "lost→won?"
        elif current_result == "won":
            direction = "won→lost?"
        elif current_result == "push":
            direction = "push→won/lost?"

        stats[direction] += 1
        at_risk.append({
            **sig,
            "pinnacle_point": pinnacle_point,
            "details_point": details_point,
            "delta": delta,
            "direction": direction,
        })

    # ── Summary ──
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Total checked:    {len(rows)}")
    print(f"  Definitely safe:  {safe}")
    print(f"  At risk:          {len(at_risk)}")
    print()
    for direction, count in sorted(stats.items()):
        print(f"    {direction:20s}  {count}")
    print()

    if not at_risk:
        print("No signals at risk of being mis-graded.")
        await db.close()
        return

    # ── Breakdown by sport and line delta ──
    by_sport = defaultdict(list)
    for s in at_risk:
        by_sport[s["sport_key"]].append(s)

    print("BREAKDOWN BY SPORT:")
    for sport, sigs in sorted(by_sport.items()):
        lost_to_won = sum(1 for s in sigs if s["direction"] == "lost→won?")
        won_to_lost = sum(1 for s in sigs if s["direction"] == "won→lost?")
        push_flip = sum(1 for s in sigs if s["direction"] == "push→won/lost?")
        deltas = [s["delta"] for s in sigs]
        avg_delta = sum(deltas) / len(deltas)
        print(f"\n  {sport}: {len(sigs)} at risk")
        print(f"    lost→won?:      {lost_to_won}")
        print(f"    won→lost?:      {won_to_lost}")
        print(f"    push→won/lost?: {push_flip}")
        print(f"    avg line delta:  {avg_delta:.1f}")
        print(f"    max line delta:  {max(deltas):.1f}")

    # ── Detail: show the at-risk signals with large deltas ──
    big_risk = [s for s in at_risk if s["delta"] >= 2.0]
    if big_risk:
        print(f"\n{'=' * 60}")
        print(f"HIGH RISK (line delta >= 2.0): {len(big_risk)} signals")
        print("=" * 60)
        for s in big_risk:
            print(f"\n  {s['signal_type']} | {s['sport_key']} | {s['market_key']}")
            print(f"    event_id:    {s['event_id']}")
            print(f"    outcome:     {s['outcome_name']}")
            print(f"    signal_at:   {s['signal_at']}")
            print(f"    graded:      {s['result']}")
            print(f"    pinnacle pt: {s['pinnacle_point']}")
            print(f"    correct pt:  {s['details_point']}")
            print(f"    delta:       {s['delta']}")
            print(f"    direction:   {s['direction']}")

    await db.close()


asyncio.run(main())
