"""CLI commands for Sharp Seeker (backtest, report, etc.)."""

from __future__ import annotations

import argparse
import asyncio
import sys

import structlog

from sharp_seeker.analysis.backtest import Backtester
from sharp_seeker.analysis.performance import PerformanceTracker
from sharp_seeker.analysis.reports import ReportGenerator
from sharp_seeker.config import Settings
from sharp_seeker.db.migrations import init_db
from sharp_seeker.db.repository import Repository
from sharp_seeker.main import configure_logging


async def run_backtest(start: str, end: str) -> None:
    settings = Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)

    db = await init_db(settings.db_path)
    repo = Repository(db)
    backtester = Backtester(settings, repo)

    result = await backtester.run(start, end)
    print(result.summary)

    await db.close()


async def run_report(period: str) -> None:
    settings = Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)

    db = await init_db(settings.db_path)
    repo = Repository(db)
    report_gen = ReportGenerator(settings, repo)

    if period == "daily":
        await report_gen.send_daily_report()
        print("Daily report sent to Discord.")
    elif period == "weekly":
        await report_gen.send_weekly_report()
        print("Weekly report sent to Discord.")

    await db.close()


async def run_stats() -> None:
    settings = Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)

    db = await init_db(settings.db_path)
    repo = Repository(db)
    tracker = PerformanceTracker(repo)

    stats = await tracker.get_stats()
    rates = await tracker.get_win_rate()

    if not stats:
        print("No resolved signals yet.")
    else:
        print("Signal Performance:")
        for st, counts in sorted(stats.items()):
            won = counts.get("won", 0)
            lost = counts.get("lost", 0)
            push = counts.get("push", 0)
            rate = rates.get(st, 0.0)
            print(f"  {st}: {rate:.1%} win rate ({won}W / {lost}L / {push}P)")

    await db.close()


def cli() -> None:
    parser = argparse.ArgumentParser(prog="sharp-seeker-tools", description="Sharp Seeker CLI tools")
    sub = parser.add_subparsers(dest="command")

    bt = sub.add_parser("backtest", help="Replay snapshots through detectors")
    bt.add_argument("start", help="Start date (ISO format, e.g. 2025-01-15)")
    bt.add_argument("end", help="End date (ISO format, e.g. 2025-01-16)")

    rp = sub.add_parser("report", help="Send a summary report to Discord")
    rp.add_argument("period", choices=["daily", "weekly"], help="Report period")

    sub.add_parser("stats", help="Show signal performance stats")

    args = parser.parse_args()

    if args.command == "backtest":
        asyncio.run(run_backtest(args.start, args.end))
    elif args.command == "report":
        asyncio.run(run_report(args.period))
    elif args.command == "stats":
        asyncio.run(run_stats())
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    cli()
