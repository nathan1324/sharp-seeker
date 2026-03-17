"""Tests for the shared hold module."""

from __future__ import annotations

from sharp_seeker.engine.hold import (
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
