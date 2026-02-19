"""API credit budget tracking and throttling."""

from __future__ import annotations

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository

log = structlog.get_logger()

CREDITS_PER_POLL = 9  # 3 sports × 3 credits each (3 markets × 1 region-equivalent)


class BudgetTracker:
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo

    async def should_poll(self) -> bool:
        """Check if we have enough budget to poll. Returns False if below 20% threshold."""
        remaining = await self._repo.get_credits_remaining()
        if remaining is None:
            return True  # no data yet, assume OK

        threshold = self._settings.odds_api_monthly_credits * 0.20
        if remaining <= threshold:
            log.warning(
                "budget_low",
                remaining=remaining,
                threshold=threshold,
                monthly=self._settings.odds_api_monthly_credits,
            )
            return False

        if remaining < CREDITS_PER_POLL:
            log.warning("budget_exhausted", remaining=remaining)
            return False

        return True

    async def get_status(self) -> dict:
        """Return current budget status."""
        remaining = await self._repo.get_credits_remaining()
        monthly = self._settings.odds_api_monthly_credits
        used = (monthly - remaining) if remaining is not None else 0
        return {
            "monthly_limit": monthly,
            "credits_remaining": remaining,
            "credits_used": used,
            "pct_remaining": round((remaining / monthly) * 100, 1) if remaining else 100.0,
        }
