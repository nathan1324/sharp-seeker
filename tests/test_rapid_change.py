"""Tests for the rapid change detector."""

from __future__ import annotations

import pytest

from sharp_seeker.engine.base import SignalType
from sharp_seeker.engine.rapid_change import RapidChangeDetector


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
        "sport_key": "americanfootball_nfl",
        "home_team": "Chiefs",
        "away_team": "Bills",
        "commence_time": "2025-01-20T00:00:00Z",
        "bookmaker_key": bookmaker,
        "market_key": market,
        "outcome_name": outcome,
        "price": price,
        "point": point,
        "fetched_at": fetched_at,
    }


@pytest.mark.asyncio
async def test_rapid_spread_change(settings, repo):
    """A 1-point spread move at one book should trigger."""
    event = "evt_rapid1"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        _snap(event, "draftkings", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "draftkings", "spreads", "Chiefs", -110, -4.0, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.signal_type == SignalType.RAPID_CHANGE
    assert sig.details["delta"] == 1.0
    assert sig.details["bookmaker"] == "draftkings"


@pytest.mark.asyncio
async def test_rapid_change_below_threshold(settings, repo):
    """A 0.25-point move should NOT trigger (threshold is 0.5)."""
    event = "evt_rapid2"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        _snap(event, "fanduel", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "fanduel", "spreads", "Chiefs", -110, -3.25, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_rapid_moneyline_change(settings, repo):
    """A 25-cent moneyline move should trigger (threshold is 20)."""
    event = "evt_rapid3"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        _snap(event, "betmgm", "h2h", "Chiefs", -150, None, t1),
        _snap(event, "betmgm", "h2h", "Chiefs", -175, None, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = RapidChangeDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    assert signals[0].details["delta"] == 25.0
