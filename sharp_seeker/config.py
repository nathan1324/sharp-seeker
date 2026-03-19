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
            "williamhill_us",
            "betrivers",
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

    # Per-sport+signal webhook overrides (JSON object in .env)
    # Keys: "signal_type:sport_key", values: webhook URL
    # Example: {"pinnacle_divergence:basketball_ncaab": "https://discord.com/api/webhooks/..."}
    discord_webhook_overrides: dict[str, str] = Field(default_factory=dict)

    # Polling
    poll_interval_minutes: int = 20

    # Detection — steam moves
    steam_min_books: int = 3
    steam_window_minutes: int = 30

    # Detection — rapid changes
    rapid_spread_threshold: float = 1.0
    rapid_ml_threshold: float = 20.0

    # Detection — Pinnacle divergence
    pinnacle_spread_threshold: float = 1.0
    pinnacle_totals_threshold: float = 1.0
    pinnacle_ml_prob_threshold: float = 0.03  # 3% implied probability edge
    pd_excluded_books: list[str] = Field(default_factory=list)
    # Per-sport Pinnacle ML probability threshold overrides (JSON object in .env)
    # Lowers or raises the ML divergence threshold for specific sports.
    # Example: {"icehockey_nhl": 0.015}
    pd_sport_ml_prob_overrides: dict[str, float] = Field(default_factory=dict)
    # Per-sport Pinnacle totals threshold overrides (JSON object in .env)
    # NHL totals are tighter (5.5-7.5) — lower threshold catches real edges.
    # Example: {"icehockey_nhl": 0.5}
    pd_sport_totals_overrides: dict[str, float] = Field(default_factory=dict)
    # Per-sport Pinnacle spread threshold overrides (JSON object in .env)
    pd_sport_spread_overrides: dict[str, float] = Field(default_factory=dict)

    # Detection — exchange monitor
    exchange_shift_threshold: float = 0.05  # 5% implied probability shift

    # Quiet hours (UTC) — skip polling during this window to save credits
    # Default: 5-14 UTC = midnight-9am ET
    quiet_hours_start: int = 5
    quiet_hours_end: int = 14

    # Minimum signal strength to alert (0.0–1.0)
    min_signal_strength: float = 0.5

    # Per-signal-type minimum strength overrides (JSON object in .env)
    # Overrides global MIN_SIGNAL_STRENGTH for specific signal types
    # Example: {"rapid_change": 0.65, "reverse_line": 0.65}
    signal_strength_overrides: dict[str, float] = Field(default_factory=dict)

    # Per-signal-type MAXIMUM strength cap (JSON object in .env)
    # Signals at or above this strength are dropped (e.g., trap signals).
    # Only applies to listed types; unlisted types have no cap.
    max_signal_strength_overrides: dict[str, float] = Field(default_factory=dict)

    # Per-signal-type + market minimum strength overrides (JSON object in .env)
    # Compound keys: "signal_type:market_key". Overrides type-level and global min.
    signal_market_strength_overrides: dict[str, float] = Field(default_factory=dict)

    # Per-signal-type + sport minimum strength overrides (JSON object in .env)
    # Compound keys: "signal_type:sport_key". Overrides type-level and global min.
    signal_sport_strength_overrides: dict[str, float] = Field(default_factory=dict)

    # Per-signal-type quiet hours (JSON object in .env)
    # Suppresses specific signal types during certain UTC hours
    # Example: {"pinnacle_divergence": [14], "reverse_line": [3, 20, 21]}
    signal_quiet_hours: dict[str, list[int]] = Field(default_factory=dict)

    # Signal blocklist — block specific type:market or type:sport:market combos (JSON array in .env)
    # 2-key patterns ("steam_move:h2h") block across all sports.
    # 3-key patterns ("rapid_change:basketball_ncaab:h2h") block only that sport.
    signal_blocklist: list[str] = Field(default_factory=list)

    # Signal best combos — highlight high-confidence type:sport:market combos (JSON array in .env)
    # 3-key patterns: "signal_type:sport_key:market_key"
    signal_best_combos: list[str] = Field(default_factory=list)

    # Signal best hours — highlight high-confidence type:hour(MST) combos (JSON object in .env)
    # Keys: signal type, values: list of MST hours (0-23)
    signal_best_hours: dict[str, list[int]] = Field(default_factory=dict)

    # Alert dedup
    alert_cooldown_minutes: int = 60

    # Database
    db_path: str = "sharp_seeker.db"

    # Card image output directory
    card_output_dir: str = "data/cards"

    # X (Twitter) — optional, disabled if credentials not set
    x_consumer_key: str | None = None
    x_consumer_secret: str | None = None
    x_access_token: str | None = None
    x_access_token_secret: str | None = None
    x_cta_url: str = ""  # Discord invite or landing page link
    x_free_play_daily_cap: int = 3  # max Elite free plays per day (2U always punches through)
    # UTC hours when teaser tweets are allowed (JSON array in .env)
    # Free play tweets are always sent regardless of this setting.
    x_teaser_hours: list[int] = Field(default_factory=list)
    x_max_strength: float = 1.0  # skip PD signals >= this strength for all tweets
    # Signal types eligible for X tweets (JSON array in .env)
    x_tweet_signal_types: list[str] = Field(
        default=["pinnacle_divergence", "rapid_change"]
    )
    x_excluded_books: list[str] = Field(default_factory=list)
    x_digest_interval_hours: int = 2  # 0 = tweet per signal (legacy)

    # Logging
    log_level: str = "INFO"
