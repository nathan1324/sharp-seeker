"""Shared test fixtures."""

from __future__ import annotations

import pytest
import aiosqlite

from sharp_seeker.db.models import SCHEMA_SQL
from sharp_seeker.db.repository import Repository
from sharp_seeker.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        odds_api_key="test_key",
        discord_webhook_url="https://discord.com/api/webhooks/test/test",
        db_path=":memory:",
    )


@pytest.fixture
async def db():
    conn = await aiosqlite.connect(":memory:")
    conn.row_factory = aiosqlite.Row
    await conn.executescript(SCHEMA_SQL)
    await conn.commit()
    yield conn
    await conn.close()


@pytest.fixture
async def repo(db) -> Repository:
    return Repository(db)
