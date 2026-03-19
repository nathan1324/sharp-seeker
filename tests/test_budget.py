"""Tests for budget tracking."""

from __future__ import annotations

import pytest

from sharp_seeker.polling.budget import BudgetTracker


@pytest.mark.asyncio
async def test_should_poll_no_data(settings, repo):
    """With no API usage data, should allow polling."""
    tracker = BudgetTracker(settings, repo)
    assert await tracker.should_poll() is True


@pytest.mark.asyncio
async def test_should_poll_plenty_credits(settings, repo):
    """With plenty of credits remaining, should allow polling."""
    await repo.record_api_usage("/sports/nba/odds", 9, 400)
    tracker = BudgetTracker(settings, repo)
    assert await tracker.should_poll() is True


@pytest.mark.asyncio
async def test_should_poll_low_budget(settings, repo):
    """Below 10% threshold should block polling."""
    # 500 monthly, 10% = 50. Recording 40 remaining should block.
    await repo.record_api_usage("/sports/nba/odds", 460, 40)
    tracker = BudgetTracker(settings, repo)
    assert await tracker.should_poll() is False


@pytest.mark.asyncio
async def test_should_poll_exhausted(settings, repo):
    """Below credits-per-poll should block."""
    await repo.record_api_usage("/sports/nba/odds", 495, 5)
    tracker = BudgetTracker(settings, repo)
    assert await tracker.should_poll() is False
