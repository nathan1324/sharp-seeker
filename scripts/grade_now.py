"""Manually trigger signal grading.

Usage:
    docker compose exec sharp-seeker python /app/scripts/grade_now.py
"""

import asyncio

import aiosqlite

from sharp_seeker.analysis.grader import ScoreGrader
from sharp_seeker.api.odds_client import OddsClient
from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository


async def grade():
    s = Settings()
    db = await aiosqlite.connect(s.db_path)
    db.row_factory = aiosqlite.Row
    repo = Repository(db)
    client = OddsClient(s)
    grader = ScoreGrader(s, client, repo)
    result = await grader.resolve_all()
    print(result)
    await db.close()


asyncio.run(grade())
