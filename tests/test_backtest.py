"""Tests for the backtesting framework."""

from __future__ import annotations

import pytest

from sharp_seeker.analysis.backtest import Backtester


def _snap(
    event_id: str,
    bookmaker: str,
    market: str,
    outcome: str,
    price: float,
    point: float | None,
    fetched_at: str,
    sport: str = "basketball_nba",
) -> dict:
    return {
        "event_id": event_id,
        "sport_key": sport,
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
async def test_backtest_finds_signals(settings, repo):
    """Backtesting over a range with steam move data should find signals."""
    event = "evt_bt1"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    snapshots = [
        _snap(event, "draftkings", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t2),
    ]
    await repo.insert_snapshots(snapshots)

    backtester = Backtester(settings, repo)
    result = await backtester.run("2025-01-15T00:00:00", "2025-01-16T00:00:00")

    assert result.fetch_cycles == 2
    assert result.total_signals > 0
    assert "steam_move" in result.signals_by_type
    assert "basketball_nba" in result.signals_by_sport


@pytest.mark.asyncio
async def test_backtest_empty_range(settings, repo):
    """Backtesting over a range with no data should return zero signals."""
    backtester = Backtester(settings, repo)
    result = await backtester.run("2099-01-01T00:00:00", "2099-01-02T00:00:00")

    assert result.fetch_cycles == 0
    assert result.total_signals == 0


@pytest.mark.asyncio
async def test_backtest_summary_output(settings, repo):
    """BacktestResult.summary should produce a readable string."""
    event = "evt_bt3"
    t1 = "2025-01-15T12:00:00+00:00"
    t2 = "2025-01-15T12:20:00+00:00"

    snapshots = [
        _snap(event, "draftkings", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t1),
        _snap(event, "draftkings", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "fanduel", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "betmgm", "spreads", "Lakers", -110, -4.0, t2),
        _snap(event, "caesars", "spreads", "Lakers", -110, -3.5, t2),
    ]
    await repo.insert_snapshots(snapshots)

    backtester = Backtester(settings, repo)
    result = await backtester.run("2025-01-15T00:00:00", "2025-01-16T00:00:00")

    summary = result.summary
    assert "Backtest:" in summary
    assert "Fetch cycles:" in summary
    assert "Total signals:" in summary
