"""SQL schema definitions for Sharp Seeker."""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS odds_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    sport_key TEXT NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    commence_time TEXT NOT NULL,
    bookmaker_key TEXT NOT NULL,
    market_key TEXT NOT NULL,
    outcome_name TEXT NOT NULL,
    price REAL NOT NULL,
    point REAL,
    fetched_at TEXT NOT NULL,
    UNIQUE(event_id, bookmaker_key, market_key, outcome_name, fetched_at)
);

CREATE TABLE IF NOT EXISTS sent_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    market_key TEXT NOT NULL,
    outcome_name TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS api_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    credits_used INTEGER NOT NULL,
    credits_remaining INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_snapshots_event_fetched
    ON odds_snapshots(event_id, fetched_at);

CREATE INDEX IF NOT EXISTS idx_snapshots_fetched
    ON odds_snapshots(fetched_at);

CREATE INDEX IF NOT EXISTS idx_alerts_dedup
    ON sent_alerts(event_id, alert_type, market_key, outcome_name, sent_at);
"""
