"""Debug the incorrectly graded NHL PD signal from 2026-02-25."""

import asyncio
import json
import aiosqlite


async def main() -> None:
    db = await aiosqlite.connect("/app/data/sharp_seeker.db")
    db.row_factory = aiosqlite.Row

    # Find the signal
    cursor = await db.execute("""
        SELECT * FROM signal_results
        WHERE signal_type = 'pinnacle_divergence'
          AND sport_key = 'icehockey_nhl'
          AND signal_at >= '2026-02-25'
          AND signal_at < '2026-02-26'
        ORDER BY signal_at DESC
    """)
    signals = await cursor.fetchall()

    if not signals:
        print("No NHL PD signals found for 2026-02-25")
        await db.close()
        return

    for sig in signals:
        sig = dict(sig)
        print("=== Signal ===")
        print(f"  event_id:     {sig['event_id']}")
        print(f"  sport_key:    {sig['sport_key']}")
        print(f"  signal_type:  {sig['signal_type']}")
        print(f"  market_key:   {sig['market_key']}")
        print(f"  outcome_name: {sig['outcome_name']}")
        print(f"  direction:    {sig['signal_direction']}")
        print(f"  strength:     {sig['signal_strength']}")
        print(f"  signal_at:    {sig['signal_at']}")
        print(f"  result:       {sig['result']}")
        print(f"  resolved_at:  {sig['resolved_at']}")

        if sig.get("details_json"):
            details = json.loads(sig["details_json"]) if isinstance(sig["details_json"], str) else sig["details_json"]
            print(f"  details:      {json.dumps(details, indent=4)}")

        event_id = sig["event_id"]
        market_key = sig["market_key"]
        outcome_name = sig["outcome_name"]
        signal_at = sig["signal_at"]

        # Check reference line
        ref_cursor = await db.execute("""
            SELECT bookmaker_key, point, price, fetched_at FROM odds_snapshots
            WHERE event_id = ? AND market_key = ? AND outcome_name = ?
              AND fetched_at <= ? AND point IS NOT NULL
              AND bookmaker_key = 'pinnacle'
            ORDER BY fetched_at DESC
            LIMIT 3
        """, (event_id, market_key, outcome_name, signal_at))
        ref_rows = await ref_cursor.fetchall()
        print(f"\n  Pinnacle ref lines for '{outcome_name}':")
        for r in ref_rows:
            r = dict(r)
            print(f"    {r['bookmaker_key']}  point={r['point']}  price={r['price']}  at={r['fetched_at']}")

        # Also check what any bookmaker line looks like
        any_cursor = await db.execute("""
            SELECT bookmaker_key, point, price, fetched_at FROM odds_snapshots
            WHERE event_id = ? AND market_key = ? AND outcome_name = ?
              AND fetched_at <= ? AND point IS NOT NULL
            ORDER BY fetched_at DESC
            LIMIT 5
        """, (event_id, market_key, outcome_name, signal_at))
        any_rows = await any_cursor.fetchall()
        print(f"\n  All bookmaker ref lines for '{outcome_name}':")
        for r in any_rows:
            r = dict(r)
            print(f"    {r['bookmaker_key']}  point={r['point']}  price={r['price']}  at={r['fetched_at']}")

        # Check event teams
        teams_cursor = await db.execute("""
            SELECT DISTINCT home_team, away_team FROM odds_snapshots
            WHERE event_id = ?
        """, (event_id,))
        teams = await teams_cursor.fetchall()
        print(f"\n  Teams in snapshots:")
        for t in teams:
            t = dict(t)
            print(f"    home={t['home_team']}  away={t['away_team']}")

        # Check both sides of the spread for this event
        both_cursor = await db.execute("""
            SELECT outcome_name, point, price, bookmaker_key, fetched_at FROM odds_snapshots
            WHERE event_id = ? AND market_key = 'spreads'
              AND fetched_at <= ? AND point IS NOT NULL
              AND bookmaker_key = 'pinnacle'
            ORDER BY fetched_at DESC
            LIMIT 10
        """, (event_id, signal_at))
        both_rows = await both_cursor.fetchall()
        print(f"\n  Both sides of spread (Pinnacle):")
        for r in both_rows:
            r = dict(r)
            print(f"    {r['outcome_name']}  point={r['point']}  price={r['price']}  at={r['fetched_at']}")

    await db.close()


asyncio.run(main())
