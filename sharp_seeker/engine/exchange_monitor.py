"""Exchange monitor: track Betfair exchange odds for significant implied probability shifts."""

from __future__ import annotations

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType

log = structlog.get_logger()

BETFAIR_KEY = "betfair_ex_eu"


def american_to_implied_prob(price: float) -> float:
    """Convert American odds to implied probability (0–1)."""
    if price > 0:
        return 100.0 / (price + 100.0)
    else:
        return abs(price) / (abs(price) + 100.0)


class ExchangeMonitorDetector(BaseDetector):
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo

    async def detect(self, event_id: str, fetched_at: str) -> list[Signal]:
        latest = await self._repo.get_latest_snapshots(event_id)
        previous = await self._repo.get_previous_snapshots(event_id, fetched_at)

        if not latest or not previous:
            return []

        # Index previous Betfair rows by (market, outcome)
        prev_map: dict[tuple[str, str], dict] = {}
        for _row in previous:
            row = dict(_row)
            if row["bookmaker_key"] == BETFAIR_KEY:
                prev_map[(row["market_key"], row["outcome_name"])] = row

        if not prev_map:
            return []

        meta: tuple[str, str, str] | None = None
        signals: list[Signal] = []

        for _row in latest:
            row = dict(_row)
            if row["bookmaker_key"] != BETFAIR_KEY:
                continue
            # Exchange data only reliable for h2h
            if row["market_key"] != "h2h":
                continue

            if meta is None:
                meta = (row["sport_key"], row["home_team"], row["away_team"])

            key = (row["market_key"], row["outcome_name"])
            prev = prev_map.get(key)
            if prev is None:
                continue

            old_prob = american_to_implied_prob(prev["price"])
            new_prob = american_to_implied_prob(row["price"])
            shift = abs(new_prob - old_prob)

            if shift < self._settings.exchange_shift_threshold:
                continue

            direction = "shortened" if new_prob > old_prob else "drifted"
            strength = min(1.0, shift / 0.15)  # 15% shift = max strength

            signals.append(
                Signal(
                    signal_type=SignalType.EXCHANGE_SHIFT,
                    event_id=event_id,
                    sport_key=meta[0],
                    home_team=meta[1],
                    away_team=meta[2],
                    market_key=row["market_key"],
                    outcome_name=row["outcome_name"],
                    strength=round(strength, 2),
                    description=(
                        f"Exchange shift: {row['outcome_name']} {direction} on Betfair "
                        f"({old_prob:.1%} → {new_prob:.1%}, shift {shift:.1%})"
                    ),
                    details={
                        "old_price": prev["price"],
                        "new_price": row["price"],
                        "old_implied_prob": round(old_prob, 4),
                        "new_implied_prob": round(new_prob, 4),
                        "shift": round(shift, 4),
                        "direction": direction,
                    },
                )
            )

        return signals
