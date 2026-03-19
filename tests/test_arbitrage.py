"""Tests for the ArbitrageDetector."""

from __future__ import annotations

import pytest

from sharp_seeker.engine.arbitrage import ArbitrageDetector
from sharp_seeker.engine.base import SignalType


class FakeSettings:
    pass


class FakeRepo:
    def __init__(self, latest_rows):
        self._latest = latest_rows

    async def get_latest_snapshots(self, event_id):
        return self._latest


def _row(bm, market, outcome, price, point=None, sport="basketball_nba",
         home="Home", away="Away", commence="2026-03-20T00:00:00Z", deep_link=None):
    return {
        "bookmaker_key": bm,
        "market_key": market,
        "outcome_name": outcome,
        "price": price,
        "point": point,
        "sport_key": sport,
        "home_team": home,
        "away_team": away,
        "commence_time": commence,
        "deep_link": deep_link,
    }


@pytest.mark.asyncio
async def test_no_arb_when_hold_positive():
    """Normal market with positive hold should produce no signals."""
    rows = [
        _row("draftkings", "h2h", "TeamA", -110),
        _row("draftkings", "h2h", "TeamB", -110),
        _row("fanduel", "h2h", "TeamA", -112),
        _row("fanduel", "h2h", "TeamB", -108),
    ]
    repo = FakeRepo(rows)
    det = ArbitrageDetector(FakeSettings(), repo)
    signals = await det.detect("evt1", "2026-03-20T01:00:00Z")
    assert signals == []


@pytest.mark.asyncio
async def test_arb_detected_when_hold_negative():
    """Cross-book hold below zero should fire an arb signal."""
    # Book A has great price on TeamA, Book B has great price on TeamB
    # implied_prob(+110) = 100/210 ≈ 0.4762
    # implied_prob(+115) = 100/215 ≈ 0.4651
    # cross_hold = 0.4651 + 0.4651 - 1 = -0.0698 (arb)
    rows = [
        _row("draftkings", "h2h", "TeamA", 115),
        _row("draftkings", "h2h", "TeamB", -110),
        _row("fanduel", "h2h", "TeamA", -105),
        _row("fanduel", "h2h", "TeamB", 115),
    ]
    repo = FakeRepo(rows)
    det = ArbitrageDetector(FakeSettings(), repo)
    signals = await det.detect("evt1", "2026-03-20T01:00:00Z")
    assert len(signals) == 1

    sig = signals[0]
    assert sig.signal_type == SignalType.ARBITRAGE
    assert sig.event_id == "evt1"
    assert sig.market_key == "h2h"
    assert sig.details["cross_book_hold"] < 0
    assert sig.details["profit_pct"] > 0

    # Check both sides are populated
    assert "side_a" in sig.details
    assert "side_b" in sig.details
    assert sig.details["side_a"]["bookmaker"] in ("draftkings", "fanduel")
    assert sig.details["side_b"]["bookmaker"] in ("draftkings", "fanduel")


@pytest.mark.asyncio
async def test_arb_no_signal_with_single_outcome():
    """A market with only one outcome should not produce signals."""
    rows = [
        _row("draftkings", "h2h", "TeamA", 115),
        _row("fanduel", "h2h", "TeamA", 120),
    ]
    repo = FakeRepo(rows)
    det = ArbitrageDetector(FakeSettings(), repo)
    signals = await det.detect("evt1", "2026-03-20T01:00:00Z")
    assert signals == []


@pytest.mark.asyncio
async def test_arb_empty_snapshots():
    """No snapshots should return empty list."""
    repo = FakeRepo([])
    det = ArbitrageDetector(FakeSettings(), repo)
    signals = await det.detect("evt1", "2026-03-20T01:00:00Z")
    assert signals == []


@pytest.mark.asyncio
async def test_arb_strength_capped_at_1():
    """Strength should be capped at 1.0 even with large negative hold."""
    # Extreme arb: both sides at +200
    # implied_prob(+200) = 100/300 = 0.333
    # cross_hold = 0.333 + 0.333 - 1 = -0.333
    rows = [
        _row("draftkings", "h2h", "TeamA", 200),
        _row("fanduel", "h2h", "TeamB", 200),
        _row("draftkings", "h2h", "TeamB", -300),
        _row("fanduel", "h2h", "TeamA", -300),
    ]
    repo = FakeRepo(rows)
    det = ArbitrageDetector(FakeSettings(), repo)
    signals = await det.detect("evt1", "2026-03-20T01:00:00Z")
    assert len(signals) == 1
    assert signals[0].strength <= 1.0


@pytest.mark.asyncio
async def test_arb_picks_best_book_each_side():
    """Arb should identify the book with the best price on each side."""
    # TeamA: DK +115 (best), FD -105
    # TeamB: DK -110, FD +115 (best)
    rows = [
        _row("draftkings", "h2h", "TeamA", 115),
        _row("draftkings", "h2h", "TeamB", -110),
        _row("fanduel", "h2h", "TeamA", -105),
        _row("fanduel", "h2h", "TeamB", 115),
    ]
    repo = FakeRepo(rows)
    det = ArbitrageDetector(FakeSettings(), repo)
    signals = await det.detect("evt1", "2026-03-20T01:00:00Z")
    assert len(signals) == 1
    sig = signals[0]

    # The best price on TeamA is DK +115, best on TeamB is FD +115
    sides = {sig.details["side_a"]["outcome"]: sig.details["side_a"],
             sig.details["side_b"]["outcome"]: sig.details["side_b"]}

    assert sides["TeamA"]["bookmaker"] == "draftkings"
    assert sides["TeamA"]["price"] == 115
    assert sides["TeamB"]["bookmaker"] == "fanduel"
    assert sides["TeamB"]["price"] == 115
