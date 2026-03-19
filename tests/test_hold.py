"""Tests for the shared hold module."""

from __future__ import annotations

from sharp_seeker.engine.hold import (
    collect_market_prices,
    compute_cross_book_hold,
    compute_hold,
    compute_hold_boost,
    compute_hold_for_book,
)


def test_compute_hold_standard_vig():
    """-110/-110 should give ~4.76% hold."""
    hold = compute_hold(-110, -110)
    assert 0.047 < hold < 0.048


def test_compute_hold_sharp_pricing():
    """-105/-105 should give ~2.44% hold."""
    hold = compute_hold(-105, -105)
    assert 0.024 < hold < 0.025


def test_compute_hold_boost_sharp():
    """Hold below 4.5% should return sharp boost."""
    assert compute_hold_boost(0.040) == 0.08


def test_compute_hold_boost_average():
    """Hold between 4.5-5.0% should return average boost."""
    assert compute_hold_boost(0.048) == 0.04


def test_compute_hold_boost_wide():
    """Hold >= 5.0% should return no boost."""
    assert compute_hold_boost(0.060) == 0.0


def test_compute_hold_boost_none():
    """None hold should return 0."""
    assert compute_hold_boost(None) == 0.0


def test_compute_hold_for_book_spreads():
    """Hold computed correctly for spreads using current_lines structure."""
    current_lines = {
        ("spreads", "Lakers", "draftkings"): {"price": -110},
        ("spreads", "Celtics", "draftkings"): {"price": -110},
    }
    hold = compute_hold_for_book(current_lines, "spreads", "Lakers", "draftkings")
    assert hold is not None
    assert 0.047 < hold < 0.048


def test_compute_hold_for_book_totals():
    """Hold computed correctly for totals (Over/Under)."""
    current_lines = {
        ("totals", "Over", "fanduel"): {"price": -105},
        ("totals", "Under", "fanduel"): {"price": -105},
    }
    hold = compute_hold_for_book(current_lines, "totals", "Over", "fanduel")
    assert hold is not None
    assert 0.024 < hold < 0.025


def test_compute_hold_for_book_missing_other_side():
    """Returns None when opposite side is not available."""
    current_lines = {
        ("spreads", "Lakers", "draftkings"): {"price": -110},
    }
    hold = compute_hold_for_book(current_lines, "spreads", "Lakers", "draftkings")
    assert hold is None


# ── Cross-book hold ──────────────────────────────────────────


def test_cross_book_hold_standard():
    """Cross-book hold with same odds = same as single-book hold."""
    hold = compute_cross_book_hold([-110, -110], [-110, -110])
    assert hold is not None
    assert 0.047 < hold < 0.048


def test_cross_book_hold_tight_market():
    """Best prices across books should give lower hold than any single book."""
    # Book A: -105/-115, Book B: -115/-105
    # Best on each side: -105/-105 → ~2.44%
    hold = compute_cross_book_hold([-105, -115], [-115, -105])
    assert hold is not None
    assert 0.024 < hold < 0.025


def test_cross_book_hold_arbitrage():
    """Negative cross-book hold = arbitrage opportunity."""
    # Very favorable odds on each side across different books
    hold = compute_cross_book_hold([+110, -110], [+110, -110])
    assert hold is not None
    assert hold < 0  # negative = arb


def test_cross_book_hold_empty_side():
    """Returns None when a side has no prices."""
    assert compute_cross_book_hold([], [-110]) is None
    assert compute_cross_book_hold([-110], []) is None


def test_cross_book_hold_single_book():
    """With one book, should match single-book hold."""
    cross = compute_cross_book_hold([-110], [-110])
    single = compute_hold(-110, -110)
    assert cross is not None
    assert abs(cross - single) < 0.0001


# ── Collect market prices ────────────────────────────────────


def test_collect_market_prices_spreads():
    """Collects prices from multiple books on both sides."""
    current_lines = {
        ("spreads", "Lakers", "draftkings"): {"price": -110},
        ("spreads", "Lakers", "fanduel"): {"price": -105},
        ("spreads", "Celtics", "draftkings"): {"price": -110},
        ("spreads", "Celtics", "fanduel"): {"price": -115},
    }
    side_a, side_b, other = collect_market_prices(current_lines, "spreads", "Lakers")
    assert sorted(side_a) == [-110, -105]
    assert sorted(side_b) == [-115, -110]
    assert other == "Celtics"


def test_collect_market_prices_totals():
    """Over/Under detection works for totals."""
    current_lines = {
        ("totals", "Over", "draftkings"): {"price": -110},
        ("totals", "Under", "draftkings"): {"price": -110},
        ("totals", "Over", "fanduel"): {"price": -108},
        ("totals", "Under", "fanduel"): {"price": -112},
    }
    side_a, side_b, other = collect_market_prices(current_lines, "totals", "Over")
    assert len(side_a) == 2
    assert len(side_b) == 2
    assert other == "Under"


def test_collect_market_prices_missing_side():
    """Returns empty lists when opposite side not found."""
    current_lines = {
        ("spreads", "Lakers", "draftkings"): {"price": -110},
    }
    side_a, side_b, other = collect_market_prices(current_lines, "spreads", "Lakers")
    assert side_a == []
    assert side_b == []
    assert other is None
