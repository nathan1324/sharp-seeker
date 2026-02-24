"""Manually trigger the daily report."""

import asyncio
import aiosqlite
from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.analysis.reports import ReportGenerator


async def main():
    settings = Settings()
    db = await aiosqlite.connect(settings.db_path)
    db.row_factory = aiosqlite.Row
    repo = Repository(db)
    gen = ReportGenerator(settings, repo)
    await gen.send_daily_report()
    await db.close()
    print("Daily report sent.")


asyncio.run(main())
