"""Database initialization and migrations."""

from __future__ import annotations

import aiosqlite
import structlog

from sharp_seeker.db.models import SCHEMA_SQL

log = structlog.get_logger()


async def _run_migrations(db: aiosqlite.Connection) -> None:
    """Apply incremental schema migrations for existing databases."""
    migrations = [
        "ALTER TABLE sent_alerts ADD COLUMN is_free_play INTEGER DEFAULT 0",
    ]
    for sql in migrations:
        try:
            await db.execute(sql)
        except Exception:
            pass  # Column already exists
    await db.commit()


async def init_db(db_path: str) -> aiosqlite.Connection:
    """Create tables if they don't exist and return a connection."""
    db = await aiosqlite.connect(db_path)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.executescript(SCHEMA_SQL)
    await db.commit()
    await _run_migrations(db)
    log.info("database_initialized", path=db_path)
    return db
