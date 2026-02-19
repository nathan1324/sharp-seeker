"""Database initialization and migrations."""

from __future__ import annotations

import aiosqlite
import structlog

from sharp_seeker.db.models import SCHEMA_SQL

log = structlog.get_logger()


async def init_db(db_path: str) -> aiosqlite.Connection:
    """Create tables if they don't exist and return a connection."""
    db = await aiosqlite.connect(db_path)
    db.row_factory = aiosqlite.Row
    await db.executescript(SCHEMA_SQL)
    await db.commit()
    log.info("database_initialized", path=db_path)
    return db
