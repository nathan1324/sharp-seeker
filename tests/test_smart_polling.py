"""Tests for smart polling priority logic."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from sharp_seeker.api.schemas import EventOddsSchema
from sharp_seeker.polling.smart import (
    PollPriority,
    classify_event,
    filter_events_for_cycle,
)


def _event(hours_from_now: float) -> EventOddsSchema:
    commence = datetime.now(timezone.utc) + timedelta(hours=hours_from_now)
    return EventOddsSchema(
        id="test_event",
        sport_key="basketball_nba",
        home_team="Lakers",
        away_team="Celtics",
        commence_time=commence.isoformat(),
        bookmakers=[],
    )


def test_classify_imminent_game():
    assert classify_event(_event(1.0)) == PollPriority.HIGH


def test_classify_mid_range_game():
    assert classify_event(_event(6.0)) == PollPriority.MEDIUM


def test_classify_far_out_game():
    assert classify_event(_event(24.0)) == PollPriority.LOW


def test_filter_cycle_1():
    """Cycle 1: all events polled (1 % 1 == 0, 1 % 2 != 0, 1 % 4 != 0)."""
    events = [_event(1), _event(6), _event(24)]
    # cycle 1: HIGH (1%1=0 yes), MEDIUM (1%2=1 no), LOW (1%4=1 no)
    result = filter_events_for_cycle(events, 1)
    assert len(result) == 1


def test_filter_cycle_2():
    """Cycle 2: HIGH and MEDIUM polled."""
    events = [_event(1), _event(6), _event(24)]
    # cycle 2: HIGH (2%1=0 yes), MEDIUM (2%2=0 yes), LOW (2%4=2 no)
    result = filter_events_for_cycle(events, 2)
    assert len(result) == 2


def test_filter_cycle_4():
    """Cycle 4: all events polled."""
    events = [_event(1), _event(6), _event(24)]
    # cycle 4: HIGH (4%1=0 yes), MEDIUM (4%2=0 yes), LOW (4%4=0 yes)
    result = filter_events_for_cycle(events, 4)
    assert len(result) == 3
