"""Tests for the Pinnacle divergence detector."""

from __future__ import annotations

import pytest

from sharp_seeker.engine.base import SignalType
from sharp_seeker.engine.pinnacle_divergence import PinnacleDivergenceDetector


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
        "fetched_at": fetched_at,
    }


@pytest.mark.asyncio
async def test_pinnacle_divergence_spread_value(settings, repo):
    """US book with better spread than Pinnacle should trigger."""
    event = "evt_pin1"
    t = "2025-01-15T12:00:00+00:00"

    # DK has -1.5 (better for bettor) vs Pinnacle -3.0 — value at DK
    snapshots = [
        _snap(event, "pinnacle", "spreads", "Lakers", -110, -3.0, t),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -1.5, t),  # 1.5 better
        _snap(event, "fanduel", "spreads", "Lakers", -110, -3.0, t),    # same as pin
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.signal_type == SignalType.PINNACLE_DIVERGENCE
    assert sig.details["us_book"] == "draftkings"
    assert sig.details["delta"] == 1.5


@pytest.mark.asyncio
async def test_pinnacle_divergence_no_signal_when_pinnacle_better(settings, repo):
    """US book with worse spread than Pinnacle should NOT trigger."""
    event = "evt_pin1b"
    t = "2025-01-15T12:00:00+00:00"

    # DK has -4.5 (worse for bettor) vs Pinnacle -3.0 — no value at DK
    snapshots = [
        _snap(event, "pinnacle", "spreads", "Lakers", -110, -3.0, t),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -4.5, t),  # 1.5 worse
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_pinnacle_divergence_moneyline_value(settings, repo):
    """US book with better ML odds than Pinnacle should trigger."""
    event = "evt_pin2"
    t = "2025-01-15T12:00:00+00:00"

    # BetMGM has -110 (better for bettor) vs Pinnacle -150 — value at BetMGM
    snapshots = [
        _snap(event, "pinnacle", "h2h", "Lakers", -150, None, t),
        _snap(event, "betmgm", "h2h", "Lakers", -110, None, t),  # 40 better
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.details["delta"] == 40.0


@pytest.mark.asyncio
async def test_no_divergence_below_threshold(settings, repo):
    """Spread diff of 0.5 should NOT trigger (threshold is 1.0)."""
    event = "evt_pin3"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "pinnacle", "spreads", "Lakers", -110, -3.0, t),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -3.5, t),
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 0


@pytest.mark.asyncio
async def test_no_signal_without_pinnacle(settings, repo):
    """No Pinnacle data means no divergence signals."""
    event = "evt_pin4"
    t = "2025-01-15T12:00:00+00:00"

    snapshots = [
        _snap(event, "draftkings", "spreads", "Lakers", -110, -4.5, t),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -3.0, t),
    ]
    await repo.insert_snapshots(snapshots)

    detector = PinnacleDivergenceDetector(settings, repo)
    signals = await detector.detect(event, t)

    assert len(signals) == 0
