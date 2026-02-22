"""Tests for the steam move detector."""

from __future__ import annotations

import pytest

from sharp_seeker.engine.base import SignalType
from sharp_seeker.engine.steam_move import SteamMoveDetector


def _snap(
    event_id: str,
    bookmaker: str,
    market: str,
    outcome: str,
    price: float,
    point: float | None,
    fetched_at: str,
) -> dict:
    return {
        "event_id": event_id,
        "sport_key": "basketball_nba",
        "home_team": "Lakers",
        "away_team": "Celtics",
        "commence_time": "2025-01-15T00:00:00Z",
        "bookmaker_key": bookmaker,
        "market_key": market,
        "outcome_name": outcome,
        "price": price,
        "point": point,
        "deep_link": None,
        "fetched_at": fetched_at,
    }


@pytest.mark.asyncio
async def test_steam_move_detected(settings, repo):
    """3 books moving a spread in the same direction triggers a steam move."""
    event = "evt1"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    snapshots = [
        # Time 1: all books have Lakers -3.5
        _snap(event, "draftkings", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t1),
        # Time 2: 3 books move to -4.0
        _snap(event, "draftkings", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t2),  # didn't move
    ]
    await repo.insert_snapshots(snapshots)

    detector = SteamMoveDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.signal_type == SignalType.STEAM_MOVE
    assert sig.details["books_moved"] == 3
    assert sig.details["direction"] == "down"  # -3.5 → -4.0 is negative delta


@pytest.mark.asyncio
async def test_no_steam_below_threshold(settings, repo):
    """Only 2 books moving should not trigger with min_books=3."""
    event = "evt2"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    snapshots = [
        _snap(event, "draftkings", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -3.5, t2),  # didn't move
    ]
    await repo.insert_snapshots(snapshots)

    detector = SteamMoveDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_steam_moneyline(settings, repo):
    """Steam move on h2h market uses price delta."""
    event = "evt3"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    snapshots = [
        _snap(event, "draftkings", "h2h", "Lakers", -150, None, t1),
        _snap(event, "fanduel", "h2h", "Lakers", -150, None, t1),
        _snap(event, "betmgm", "h2h", "Lakers", -150, None, t1),
        _snap(event, "draftkings", "h2h", "Lakers", -170, None, t2),
        _snap(event, "fanduel", "h2h", "Lakers", -175, None, t2),
        _snap(event, "betmgm", "h2h", "Lakers", -165, None, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = SteamMoveDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    assert signals[0].details["direction"] == "down"
