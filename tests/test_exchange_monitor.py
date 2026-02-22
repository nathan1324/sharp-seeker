"""Tests for the exchange monitor detector."""

from __future__ import annotations

import pytest

from sharp_seeker.engine.base import SignalType
from sharp_seeker.engine.exchange_monitor import (
    ExchangeMonitorDetector,
    american_to_implied_prob,
)


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


def test_american_to_implied_prob():
    """Test odds conversion."""
    assert abs(american_to_implied_prob(-200) - 0.6667) < 0.001
    assert abs(american_to_implied_prob(200) - 0.3333) < 0.001
    assert abs(american_to_implied_prob(-100) - 0.5) < 0.001
    assert abs(american_to_implied_prob(100) - 0.5) < 0.001


@pytest.mark.asyncio
async def test_exchange_shift_detected(settings, repo):
    """A large Betfair price move should trigger an exchange shift signal."""
    event = "evt_ex1"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    # -150 → ~60% implied, -250 → ~71% implied → ~11% shift
    snapshots = [
        _snap(event, "betfair_ex_eu", "h2h", "Lakers", -150, None, t1),
        _snap(event, "betfair_ex_eu", "h2h", "Lakers", -250, None, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = ExchangeMonitorDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.signal_type == SignalType.EXCHANGE_SHIFT
    assert sig.details["direction"] == "shortened"
    assert sig.details["shift"] > 0.05


@pytest.mark.asyncio
async def test_exchange_shift_below_threshold(settings, repo):
    """A small Betfair price move should NOT trigger."""
    event = "evt_ex2"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    # -150 → -155: very small shift
    snapshots = [
        _snap(event, "betfair_ex_eu", "h2h", "Lakers", -150, None, t1),
        _snap(event, "betfair_ex_eu", "h2h", "Lakers", -155, None, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = ExchangeMonitorDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_exchange_ignores_non_betfair(settings, repo):
    """Only Betfair data should be considered."""
    event = "evt_ex3"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    snapshots = [
        _snap(event, "draftkings", "h2h", "Lakers", -150, None, t1),
        _snap(event, "draftkings", "h2h", "Lakers", -250, None, t2),
    ]
    await repo.insert_snapshots(snapshots)

    detector = ExchangeMonitorDetector(settings, repo)
    signals = await detector.detect(event, t2)

    assert len(signals) == 0
