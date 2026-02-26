"""Re-grade mismatched spread/total signals using actual game scores.

Fetches scores for the last 3 days (API limit), finds resolved signals
where the details_json point differs from Pinnacle's, re-grades with
the correct point, and fixes any that changed.
"""

import asyncio
import json
from datetime import datetime, timezone

import aiosqlite
import httpx


API_BASE = "https://api.the-odds-api.com/v4"
DAYS_FROM = 3


def grade_spread(outcome_name: str, game: dict, point: float) -> str:
    scores = {s["name"]: int(s["score"]) for s in game["scores"]}
    home = game["home_team"]
    away = game["away_team"]
    if outcome_name == home:
        margin = scores.get(home, 0) - scores.get(away, 0)
    elif outcome_name == away:
        margin = scores.get(away, 0) - scores.get(home, 0)
    else:
        return "push"
    adjusted = margin + point
    if adjusted > 0:
        return "won"
    elif adjusted < 0:
        return "lost"
    return "push"


def grade_total(outcome_name: str, game: dict, point: float) -> str:
    scores = {s["name"]: int(s["score"]) for s in game["scores"]}
    combined = sum(scores.values())
    if combined > point:
        return "won" if outcome_name == "Over" else "lost"
    elif combined < point:
        return "won" if outcome_name == "Under" else "lost"
    return "push"


async def main() -> None:
    db = await aiosqlite.connect("/app/data/sharp_seeker.db")
    db.row_factory = aiosqlite.Row

    # Get API key from env
    import os
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        # Try reading from .env file
        env_path = "/app/.env"
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("ODDS_API_KEY="):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
    if not api_key:
        print("ERROR: ODDS_API_KEY not found")
        await db.close()
        return

    # Get all sport keys from mismatched signals
    cursor = await db.execute("""
        SELECT DISTINCT sport_key FROM signal_results
        WHERE result IS NOT NULL
          AND market_key IN ('spreads', 'totals')
          AND details_json IS NOT NULL
          AND sport_key IS NOT NULL
    """)
    sport_rows = await cursor.fetchall()
    sport_keys = [r["sport_key"] for r in sport_rows]
    print(f"Sports to check: {sport_keys}\n")

    # Fetch scores for each sport
    scores_by_event: dict[str, dict] = {}
    async with httpx.AsyncClient(base_url=API_BASE, timeout=30) as client:
        for sport_key in sport_keys:
            try:
                resp = await client.get(
                    f"/sports/{sport_key}/scores",
                    params={"apiKey": api_key, "daysFrom": DAYS_FROM},
                )
                resp.raise_for_status()
                games = resp.json()
                for game in games:
                    if game.get("scores") and game.get("completed"):
                        scores_by_event[game["id"]] = game
                print(f"  {sport_key}: {len(games)} games, "
                      f"{sum(1 for g in games if g.get('completed'))} completed")
            except Exception as e:
                print(f"  {sport_key}: ERROR fetching scores — {e}")

    print(f"\nTotal completed games with scores: {len(scores_by_event)}\n")

    # Get all resolved spread/total signals with details_json
    cursor = await db.execute("""
        SELECT * FROM signal_results
        WHERE result IS NOT NULL
          AND market_key IN ('spreads', 'totals')
          AND details_json IS NOT NULL
        ORDER BY signal_at DESC
    """)
    rows = await cursor.fetchall()

    checked = 0
    no_scores = 0
    correct = 0
    fixed = 0
    fixes = []

    for row in rows:
        sig = dict(row)
        event_id = sig["event_id"]
        market_key = sig["market_key"]
        outcome_name = sig["outcome_name"]
        signal_at = sig["signal_at"]
        current_result = sig["result"]

        # Extract details_json point
        try:
            details = json.loads(sig["details_json"])
            value_books = details.get("value_books", [])
            if not value_books or value_books[0].get("point") is None:
                continue
            details_point = float(value_books[0]["point"])
        except (json.JSONDecodeError, TypeError, ValueError):
            continue

        # Get Pinnacle reference line
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
        if details_point == pinnacle_point:
            continue

        # Lines differ — do we have scores for this event?
        game = scores_by_event.get(event_id)
        if not game:
            no_scores += 1
            continue

        checked += 1

        # Re-grade with the correct point
        if market_key == "spreads":
            correct_result = grade_spread(outcome_name, game, details_point)
        elif market_key == "totals":
            correct_result = grade_total(outcome_name, game, details_point)
        else:
            continue

        if correct_result == current_result:
            correct += 1
        else:
            fixed += 1
            fixes.append({
                "event_id": event_id,
                "signal_type": sig["signal_type"],
                "market_key": market_key,
                "outcome_name": outcome_name,
                "signal_at": signal_at,
                "old_result": current_result,
                "new_result": correct_result,
                "pinnacle_point": pinnacle_point,
                "details_point": details_point,
                "sport_key": sig["sport_key"],
            })
            print(f"FIX: {sig['signal_type']} | {sig['sport_key']} | {market_key}")
            print(f"  {outcome_name}")
            print(f"  pinnacle: {pinnacle_point}  →  correct: {details_point}")
            print(f"  {current_result} → {correct_result}")
            print()

    # Apply fixes
    if fixes:
        now = datetime.now(timezone.utc).isoformat()
        for f in fixes:
            await db.execute("""
                UPDATE signal_results SET result = ?, resolved_at = ?
                WHERE event_id = ? AND signal_type = ? AND market_key = ?
                  AND outcome_name = ? AND signal_at = ?
            """, (f["new_result"], now, f["event_id"], f["signal_type"],
                  f["market_key"], f["outcome_name"], f["signal_at"]))
        await db.commit()

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Mismatched signals with scores available: {checked}")
    print(f"  Mismatched signals without scores (too old): {no_scores}")
    print(f"  Already correct despite line mismatch: {correct}")
    print(f"  Fixed (grade changed): {fixed}")
    if fixes:
        for f in fixes:
            print(f"    {f['sport_key']} | {f['outcome_name']} | "
                  f"{f['old_result']}→{f['new_result']} "
                  f"(pinnacle {f['pinnacle_point']} → correct {f['details_point']})")
    print("=" * 60)

    await db.close()


asyncio.run(main())
