"""Pinnacle divergence detector: US books diverge significantly from Pinnacle's line."""

from __future__ import annotations

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType

log = structlog.get_logger()

PINNACLE_KEY = "pinnacle"
US_BOOKS = {"draftkings", "fanduel", "betmgm", "caesars", "williamhill_us"}


class PinnacleDivergenceDetector(BaseDetector):
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo

    async def detect(self, event_id: str, fetched_at: str) -> list[Signal]:
        latest = await self._repo.get_latest_snapshots(event_id)
        if not latest:
            return []

        # Index by (market, outcome) → {bookmaker: row}
        by_market: dict[tuple[str, str], dict[str, dict]] = {}
        meta: tuple[str, str, str] | None = None

        for _row in latest:
            row = dict(_row)
            key = (row["market_key"], row["outcome_name"])
            by_market.setdefault(key, {})[row["bookmaker_key"]] = row
            if meta is None:
                meta = (row["sport_key"], row["home_team"], row["away_team"])

        if meta is None:
            return []

        signals: list[Signal] = []

        for (market_key, outcome_name), books in by_market.items():
            pinnacle = books.get(PINNACLE_KEY)
            if pinnacle is None:
                continue

            for bm_key, row in books.items():
                if bm_key not in US_BOOKS:
                    continue

                if market_key == "h2h":
                    delta = abs(row["price"] - pinnacle["price"])
                    threshold = self._settings.pinnacle_ml_threshold
                    pin_label = f"{pinnacle['price']:+.0f}"
                    us_label = f"{row['price']:+.0f}"
                else:
                    if row["point"] is not None and pinnacle["point"] is not None:
                        delta = abs(row["point"] - pinnacle["point"])
                        threshold = self._settings.pinnacle_spread_threshold
                        pin_label = str(pinnacle["point"])
                        us_label = str(row["point"])
                    else:
                        continue

                if delta < threshold:
                    continue

                strength = min(1.0, delta / (threshold * 3))

                signals.append(
                    Signal(
                        signal_type=SignalType.PINNACLE_DIVERGENCE,
                        event_id=event_id,
                        sport_key=meta[0],
                        home_team=meta[1],
                        away_team=meta[2],
                        market_key=market_key,
                        outcome_name=outcome_name,
                        strength=round(strength, 2),
                        description=(
                            f"Pinnacle divergence: {bm_key} has {outcome_name} "
                            f"at {us_label} vs Pinnacle {pin_label} "
                            f"({market_key}, delta {delta:.1f})"
                        ),
                        details={
                            "us_book": bm_key,
                            "us_value": row["point"] if market_key != "h2h" else row["price"],
                            "pinnacle_value": pinnacle["point"] if market_key != "h2h" else pinnacle["price"],
                            "delta": round(delta, 2),
                        },
                    )
                )

        return signals
