from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # The Odds API
    odds_api_key: str
    odds_api_base_url: str = "https://api.the-odds-api.com/v4"
    odds_api_monthly_credits: int = 500

    # Target bookmakers (JSON array in .env)
    bookmakers: list[str] = Field(
        default=[
            "draftkings",
            "fanduel",
            "betmgm",
            "caesars",
            "williamhill_us",
            "pinnacle",
            "betfair_ex_eu",
        ]
    )

    # Sports to track (JSON array in .env)
    sports: list[str] = Field(
        default=["americanfootball_nfl", "basketball_nba", "baseball_mlb"]
    )

    # Discord
    discord_webhook_url: str

    # Polling
    poll_interval_minutes: int = 20

    # Detection — steam moves
    steam_min_books: int = 3
    steam_window_minutes: int = 30

    # Detection — rapid changes
    rapid_spread_threshold: float = 0.5
    rapid_ml_threshold: float = 20.0

    # Alert dedup
    alert_cooldown_minutes: int = 60

    # Database
    db_path: str = "sharp_seeker.db"

    # Logging
    log_level: str = "INFO"
