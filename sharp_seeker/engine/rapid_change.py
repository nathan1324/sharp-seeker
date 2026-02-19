"""Rapid change detector: single book moves a line by a large amount between polls."""

from __future__ import annotations

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType

log = structlog.get_logger()


class RapidChangeDetector(BaseDetector):
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo

    async def detect(self, event_id: str, fetched_at: str) -> list[Signal]:
        latest = await self._repo.get_latest_snapshots(event_id)
        previous = await self._repo.get_previous_snapshots(event_id, fetched_at)

        if not latest or not previous:
            return []

        # Index previous by (bookmaker, market, outcome)
        prev_map: dict[tuple[str, str, str], dict] = {}
        for row in previous:
            key = (row["bookmaker_key"], row["market_key"], row["outcome_name"])
            prev_map[key] = dict(row)

        meta: tuple[str, str, str] | None = None
        signals: list[Signal] = []

        for _row in latest:
            row = dict(_row)
            key = (row["bookmaker_key"], row["market_key"], row["outcome_name"])
            prev = prev_map.get(key)
            if prev is None:
                continue

            if meta is None:
                meta = (row["sport_key"], row["home_team"], row["away_team"])

            market_key = row["market_key"]
            bm = row["bookmaker_key"]

            if market_key == "h2h":
                delta = abs(row["price"] - prev["price"])
                threshold = self._settings.rapid_ml_threshold
                label = f"{prev['price']:+.0f} → {row['price']:+.0f}"
            else:
                if row["point"] is not None and prev["point"] is not None:
                    delta = abs(row["point"] - prev["point"])
                    threshold = self._settings.rapid_spread_threshold
                    label = f"{prev['point']} → {row['point']}"
                else:
                    continue

            if delta < threshold:
                continue

            strength = min(1.0, delta / (threshold * 3))

            signals.append(
                Signal(
                    signal_type=SignalType.RAPID_CHANGE,
                    event_id=event_id,
                    sport_key=meta[0],
                    home_team=meta[1],
                    away_team=meta[2],
                    market_key=market_key,
                    outcome_name=row["outcome_name"],
                    strength=round(strength, 2),
                    description=(
                        f"Rapid change at {bm}: {row['outcome_name']} "
                        f"({market_key}) {label} (delta {delta:.1f})"
                    ),
                    details={
                        "bookmaker": bm,
                        "old_price": prev["price"],
                        "new_price": row["price"],
                        "old_point": prev.get("point"),
                        "new_point": row.get("point"),
                        "delta": round(delta, 2),
                    },
                )
            )

        return signals
