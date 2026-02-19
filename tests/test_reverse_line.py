"""Tests for the reverse line movement detector."""

from __future__ import annotations

import pytest

from sharp_seeker.engine.base import SignalType
from sharp_seeker.engine.reverse_line import ReverseLineDetector


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
async def test_reverse_line_detected(settings, repo):
    """US books move spread down but Pinnacle moves up → RLM signal."""
    event = "evt_rlm1"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        # Time 1
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "draftkings", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "fanduel", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "betmgm", "spreads", "Chiefs", -110, -3.0, t1),
        # Time 2: US books go down, Pinnacle goes up
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -2.5, t2),  # up (+0.5)
        _snap(event, "draftkings", "spreads", "Chiefs", -110, -3.5, t2),  # down (-0.5)
        _snap(event, "fanduel", "spreads", "Chiefs", -110, -3.5, t2),    # down (-0.5)
        _snap(event, "betmgm", "spreads", "Chiefs", -110, -4.0, t2),     # down (-1.0)
    ]
    await repo.insert_snapshots(snapshots)

    detector = ReverseLineDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.signal_type == SignalType.REVERSE_LINE
    assert sig.details["us_direction"] == "down"
    assert sig.details["pinnacle_direction"] == "up"


@pytest.mark.asyncio
async def test_no_rlm_same_direction(settings, repo):
    """If US and Pinnacle move the same direction, no RLM signal."""
    event = "evt_rlm2"
    t1 = "2025-01-20T12:00:00+00:00"
    t2 = "2025-01-20T12:20:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "draftkings", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "fanduel", "spreads", "Chiefs", -110, -3.0, t1),
        _snap(event, "pinnacle", "spreads", "Chiefs", -110, -3.5, t2),
        _snap(event, "draftkings", "spreads", "Chiefs", -110, -3.5, t2),
        _snap(event, "fanduel", "spreads", "Chiefs", -110, -3.5, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = ReverseLineDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 0
