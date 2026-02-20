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
            "pinnacle",
        ]
    )

    # Sports to track (JSON array in .env)
    sports: list[str] = Field(
        default=["basketball_nba"]
    )

    # Discord — default webhook (required), per-signal-type webhooks (optional)
    discord_webhook_url: str
    discord_webhook_steam_move: str | None = None
    discord_webhook_rapid_change: str | None = None
    discord_webhook_pinnacle_divergence: str | None = None
    discord_webhook_reverse_line: str | None = None
    discord_webhook_exchange_shift: str | None = None

    # Polling
    poll_interval_minutes: int = 20

    # Detection — steam moves
    steam_min_books: int = 3
    steam_window_minutes: int = 30

    # Detection — rapid changes
    rapid_spread_threshold: float = 0.5
    rapid_ml_threshold: float = 20.0

    # Detection — Pinnacle divergence
    pinnacle_spread_threshold: float = 1.0
    pinnacle_ml_prob_threshold: float = 0.03  # 3% implied probability edge

    # Detection — exchange monitor
    exchange_shift_threshold: float = 0.05  # 5% implied probability shift

    # Quiet hours (UTC) — skip polling during this window to save credits
    # Default: 5-14 UTC = midnight-9am ET
    quiet_hours_start: int = 5
    quiet_hours_end: int = 14

    # Minimum signal strength to alert (0.0–1.0)
    min_signal_strength: float = 0.5

    # Alert dedup
    alert_cooldown_minutes: int = 60

    # Database
    db_path: str = "sharp_seeker.db"

    # Logging
    log_level: str = "INFO"
