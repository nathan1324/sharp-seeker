"""Entry point for Sharp Seeker."""

from __future__ import annotations

import asyncio
import signal
import sys

import structlog

from sharp_seeker.alerts.discord import DiscordAlerter
from sharp_seeker.analysis.grader import ScoreGrader
from sharp_seeker.analysis.performance import PerformanceTracker
from sharp_seeker.analysis.reports import ReportGenerator
from sharp_seeker.api.odds_client import OddsClient
from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.pipeline import DetectionPipeline
from sharp_seeker.polling.budget import BudgetTracker
from sharp_seeker.polling.scheduler import Poller, create_scheduler


def configure_logging(level: str) -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer() if sys.stderr.isatty() else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            structlog._log_levels.NAME_TO_LEVEL[level.lower()]
        ),
    )


async def run() -> None:
    settings = Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)

    log = structlog.get_logger()
    log.info("starting", version="0.3.0")

    db = await init_db(settings.db_path)
    repo = Repository(db)
    odds_client = OddsClient(settings, repo)
    pipeline = DetectionPipeline(settings, repo)
    alerter = DiscordAlerter(settings, repo)
    budget = BudgetTracker(settings, repo)
    perf_tracker = PerformanceTracker(repo)
    report_gen = ReportGenerator(settings, repo)
    grader = ScoreGrader(settings, odds_client, repo)

    poller = Poller(
        settings, odds_client, pipeline, alerter, budget, perf_tracker, report_gen,
        grader,
    )
    scheduler = create_scheduler(poller, settings)

    stop_event = asyncio.Event()

    def handle_shutdown(*_: object) -> None:
        log.info("shutdown_requested")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_shutdown)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for SIGTERM
            pass

    scheduler.start()
    log.info("scheduler_started", interval_minutes=settings.poll_interval_minutes)

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        scheduler.shutdown(wait=False)
        await odds_client.close()
        await db.close()
        log.info("shutdown_complete")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
