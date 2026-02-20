"""APScheduler-based polling scheduler."""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from sharp_seeker.alerts.discord import DiscordAlerter
from sharp_seeker.analysis.grader import ScoreGrader
from sharp_seeker.analysis.performance import PerformanceTracker
from sharp_seeker.analysis.reports import ReportGenerator
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
        perf_tracker: PerformanceTracker,
        report_gen: ReportGenerator,
        grader: ScoreGrader,
    ) -> None:
        self._settings = settings
        self._odds_client = odds_client
        self._pipeline = pipeline
        self._alerter = alerter
        self._budget = budget
        self._perf_tracker = perf_tracker
        self._report_gen = report_gen
        self._grader = grader
        self._cycle_count = 0

    async def poll_cycle(self) -> None:
        """Execute one full poll → detect → alert → track cycle."""
        now = datetime.now(timezone.utc)
        start = self._settings.quiet_hours_start
        end = self._settings.quiet_hours_end

        if start < end:
            in_quiet = start <= now.hour < end
        else:
            in_quiet = now.hour >= start or now.hour < end

        if in_quiet:
            log.info("poll_skipped_quiet_hours", hour_utc=now.hour)
            return

        self._cycle_count += 1
        log.info("poll_cycle_start", cycle=self._cycle_count)

        if not await self._budget.should_poll():
            log.warning("poll_skipped_budget")
            return

        try:
            fetched_at, results = await self._odds_client.fetch_all_sports_odds(
                cycle_count=self._cycle_count
            )
        except Exception:
            log.exception("poll_fetch_error")
            return

        if not results:
            log.info("poll_no_data")
            return

        signals = await self._pipeline.run(fetched_at)

        if signals:
            await self._alerter.send_signals(signals)
            await self._perf_tracker.record_signals(signals, fetched_at)
            log.info("poll_cycle_alerts", count=len(signals))
        else:
            log.info("poll_cycle_no_signals")

    async def daily_summary(self) -> None:
        """Send daily budget summary to Discord."""
        try:
            await self._budget.send_daily_summary()
        except Exception:
            log.exception("daily_summary_error")

    async def daily_report(self) -> None:
        """Send daily signal performance report."""
        try:
            await self._report_gen.send_daily_report()
        except Exception:
            log.exception("daily_report_error")

    async def weekly_report(self) -> None:
        """Send weekly signal performance report."""
        try:
            await self._report_gen.send_weekly_report()
        except Exception:
            log.exception("weekly_report_error")

    async def resolve_signals(self) -> None:
        """Grade unresolved signals against final game scores."""
        try:
            counts = await self._grader.resolve_all()
            log.info("resolve_signals_done", **counts)
        except Exception:
            log.exception("resolve_signals_error")


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

    # Daily budget summary at midnight UTC
    scheduler.add_job(
        poller.daily_summary,
        "cron",
        hour=0,
        minute=0,
        id="daily_summary",
        name="Send daily budget summary",
    )

    # Resolve signals at 14:00 UTC (7 AM MT) — grade yesterday's games
    scheduler.add_job(
        poller.resolve_signals,
        "cron",
        hour=14,
        minute=0,
        id="resolve_signals",
        name="Grade signals against final scores",
    )

    # Daily signal performance report at 15:00 UTC (8 AM MT) — after grading
    scheduler.add_job(
        poller.daily_report,
        "cron",
        hour=15,
        minute=0,
        id="daily_report",
        name="Send daily signal report",
    )

    # Weekly report every Monday at 15:30 UTC (8:30 AM MT)
    scheduler.add_job(
        poller.weekly_report,
        "cron",
        day_of_week="mon",
        hour=15,
        minute=30,
        id="weekly_report",
        name="Send weekly signal report",
    )

    return scheduler
