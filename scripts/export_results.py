"""Export yesterday's signal results as CSV."""

import asyncio
import csv
import json
import sys
from datetime import datetime, timedelta, timezone

from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository


def _parse_best_book(d):
    """Extract best book's odds components from details_json."""
    details_raw = d.get("details_json")
    if not details_raw:
        return "", "", ""
    try:
        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        vb = details.get("value_books", [])
        if not vb:
            return "", "", ""
        best = vb[0]
        return (
            best.get("bookmaker", ""),
            best.get("point", ""),
            best.get("price", ""),
        )
    except (json.JSONDecodeError, TypeError):
        return "", "", ""


def _sport_label(sport_key):
    parts = sport_key.split("_", 1)
    return parts[-1].upper() if len(parts) > 1 else sport_key.upper()


async def main():
    s = Settings()
    db = await init_db(s.db_path)
    repo = Repository(db)

    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    writer = csv.writer(sys.stdout)
    writer.writerow([
        "result", "sport", "matchup", "signal_type", "market",
        "outcome", "book", "point", "price", "strength", "signal_at",
    ])

    # Resolved signals
    resolved = await repo.get_resolved_signals_since(since)
    for row in resolved:
        d = dict(row)
        teams = await repo.get_event_teams(d["event_id"])
        matchup = f"{teams[1]} vs {teams[0]}" if teams else d["event_id"]
        book, point, price = _parse_best_book(d)
        writer.writerow([
            d["result"].upper(),
            _sport_label(d.get("sport_key", "")),
            matchup,
            d["signal_type"],
            d["market_key"],
            d["outcome_name"],
            book,
            point,
            price,
            d["signal_strength"],
            d.get("signal_at", ""),
        ])

    # Unresolved signals
    unresolved = await repo.get_unresolved_signals()
    for row in unresolved:
        d = dict(row)
        teams = await repo.get_event_teams(d["event_id"])
        matchup = f"{teams[1]} vs {teams[0]}" if teams else d["event_id"]
        book, point, price = _parse_best_book(d)
        writer.writerow([
            "PENDING",
            _sport_label(d.get("sport_key", "")),
            matchup,
            d["signal_type"],
            d["market_key"],
            d["outcome_name"],
            book,
            point,
            price,
            d["signal_strength"],
            d.get("signal_at", ""),
        ])

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
