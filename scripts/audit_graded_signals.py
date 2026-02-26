"""Audit all resolved spread/total signals for grading errors.

Checks whether any resolved signal was graded against Pinnacle's line
instead of the recommended bet's line from details_json.  Reports any
signals where the correct line would have produced a different result.
"""

import asyncio
import json

import aiosqlite


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

    misgraded = []

    for row in rows:
        sig = dict(row)
        event_id = sig["event_id"]
        market_key = sig["market_key"]
        outcome_name = sig["outcome_name"]
        signal_at = sig["signal_at"]
        current_result = sig["result"]

        # Extract the recommended bet's point from details_json
        try:
            details = json.loads(sig["details_json"])
            value_books = details.get("value_books", [])
            if not value_books or value_books[0].get("point") is None:
                continue
            details_point = float(value_books[0]["point"])
        except (json.JSONDecodeError, TypeError, ValueError):
            continue

        # Get the Pinnacle reference line that get_reference_line() would have used
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
            continue

        pinnacle_point = float(ref_row["point"])

        # If points match, grading was correct regardless
        if details_point == pinnacle_point:
            continue

        # Points differ — check if the result would change with the correct line
        # We need the game scores to re-grade
        # Look up teams from snapshots
        teams_cursor = await db.execute("""
            SELECT DISTINCT home_team, away_team FROM odds_snapshots
            WHERE event_id = ?
        """, (event_id,))
        teams_row = await teams_cursor.fetchone()
        if not teams_row:
            continue
        teams = dict(teams_row)

        # We don't have scores stored locally, so flag any where lines differ
        print(f"MISMATCH: {sig['signal_type']} | {sig['sport_key']} | {market_key}")
        print(f"  event_id:     {event_id}")
        print(f"  outcome:      {outcome_name}")
        print(f"  signal_at:    {signal_at}")
        print(f"  teams:        {teams['home_team']} vs {teams['away_team']}")
        print(f"  details point (recommended bet): {details_point}")
        print(f"  pinnacle point (used by grader):  {pinnacle_point}")
        print(f"  current result: {current_result}")
        print()

        misgraded.append(sig)

    if not misgraded:
        print("No mismatches found — all signals were graded with the correct line.")
    else:
        print(f"Found {len(misgraded)} signal(s) with mismatched lines.")
        print("These may have been graded incorrectly.")

    await db.close()


asyncio.run(main())
