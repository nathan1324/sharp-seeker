"""APScheduler-based polling scheduler."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from sharp_seeker.alerts.discord import DiscordAlerter
from sharp_seeker.api.odds_client import OddsClient
from sharp_seeker.config import Settings
from sharp_seeker.engine.pipeline import DetectionPipeline
from sharp_seeker.polling.budget import BudgetTracker

log = structlog.get_logger()


class Poller:
    def __init__(
        self,
        settings: Settings,
        odds_client: OddsClient,
        pipeline: DetectionPipeline,
        alerter: DiscordAlerter,
        budget: BudgetTracker,
    ) -> None:
        self._settings = settings
        self._odds_client = odds_client
        self._pipeline = pipeline
        self._alerter = alerter
        self._budget = budget

    async def poll_cycle(self) -> None:
        """Execute one full poll → detect → alert cycle."""
        log.info("poll_cycle_start")

        if not await self._budget.should_poll():
            log.warning("poll_skipped_budget")
            return

        try:
            results = await self._odds_client.fetch_all_sports_odds()
        except Exception:
            log.exception("poll_fetch_error")
            return

        if not results:
            log.info("poll_no_data")
            return

        fetched_at = datetime.now(timezone.utc).isoformat()
        signals = await self._pipeline.run(fetched_at)

        if signals:
            await self._alerter.send_signals(signals)
            log.info("poll_cycle_alerts", count=len(signals))
        else:
            log.info("poll_cycle_no_signals")


def create_scheduler(poller: Poller, settings: Settings) -> AsyncIOScheduler:
    """Create and configure the APScheduler instance."""
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        poller.poll_cycle,
        "interval",
        minutes=settings.poll_interval_minutes,
        id="poll_odds",
        name="Poll odds and detect signals",
        next_run_time=datetime.now(timezone.utc),  # run immediately on start
    )

    return scheduler
