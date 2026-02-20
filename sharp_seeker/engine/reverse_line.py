"""Reverse line movement detector: US consensus moves opposite to Pinnacle."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType

log = structlog.get_logger()

PINNACLE_KEY = "pinnacle"
US_BOOKS = {"draftkings", "fanduel", "betmgm", "caesars", "williamhill_us"}


class ReverseLineDetector(BaseDetector):
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo

    async def detect(self, event_id: str, fetched_at: str) -> list[Signal]:
        window_start = (
            datetime.fromisoformat(fetched_at)
            - timedelta(minutes=self._settings.steam_window_minutes)
        ).isoformat()

        snapshots = await self._repo.get_snapshots_since(event_id, window_start)
        if not snapshots:
            return []

        # Group by (market, outcome) → {bookmaker → list of (fetched_at, price, point)}
        grouped: dict[tuple[str, str], dict[str, list[tuple[str, float, float | None]]]] = (
            defaultdict(lambda: defaultdict(list))
        )
        meta: tuple[str, str, str] | None = None

        for snap in snapshots:
            key = (snap["market_key"], snap["outcome_name"])
            grouped[key][snap["bookmaker_key"]].append(
                (snap["fetched_at"], snap["price"], snap["point"])
            )
            if meta is None:
                meta = (snap["sport_key"], snap["home_team"], snap["away_team"])

        if meta is None:
            return []

        # Build current lines for value book detection
        latest = await self._repo.get_latest_snapshots(event_id)
        current_lines: dict[tuple[str, str, str], dict] = {}
        for _row in latest:
            row = dict(_row)
            mk, on, bm = row["market_key"], row["outcome_name"], row["bookmaker_key"]
            current_lines[(mk, on, bm)] = row

        signals: list[Signal] = []

        for (market_key, outcome_name), book_data in grouped.items():
            # Get Pinnacle's movement direction
            pin_entries = book_data.get(PINNACLE_KEY)
            if not pin_entries or len(pin_entries) < 2:
                continue

            pin_entries.sort(key=lambda e: e[0])
            pin_delta = self._calc_delta(market_key, pin_entries[0], pin_entries[-1])
            if pin_delta == 0:
                continue

            # Get US consensus direction (average delta across US books that moved)
            us_deltas: list[float] = []
            us_movers: list[str] = []
            for bm_key, entries in book_data.items():
                if bm_key not in US_BOOKS or len(entries) < 2:
                    continue
                entries.sort(key=lambda e: e[0])
                delta = self._calc_delta(market_key, entries[0], entries[-1])
                if delta != 0:
                    us_deltas.append(delta)
                    us_movers.append(bm_key)

            if len(us_deltas) < 2:
                continue

            us_avg = sum(us_deltas) / len(us_deltas)

            # Reverse line movement: US consensus and Pinnacle move opposite directions
            if (us_avg > 0 and pin_delta < 0) or (us_avg < 0 and pin_delta > 0):
                us_dir = "up" if us_avg > 0 else "down"
                pin_dir = "up" if pin_delta > 0 else "down"
                strength = min(1.0, (abs(us_avg) + abs(pin_delta)) / 4.0)

                # Value: US books moved the WRONG way — bet in Pinnacle's direction
                # at US books (they have the "wrong" line)
                value_books: list[dict] = []
                for bm_key in us_movers:
                    current = current_lines.get((market_key, outcome_name, bm_key))
                    if current is not None:
                        value_books.append({
                            "bookmaker": bm_key,
                            "price": current["price"],
                            "point": current.get("point"),
                        })

                signals.append(
                    Signal(
                        signal_type=SignalType.REVERSE_LINE,
                        event_id=event_id,
                        sport_key=meta[0],
                        home_team=meta[1],
                        away_team=meta[2],
                        market_key=market_key,
                        outcome_name=outcome_name,
                        strength=round(strength, 2),
                        description=(
                            f"Reverse line movement: US consensus moved {us_dir} "
                            f"(avg {us_avg:+.2f}) but Pinnacle moved {pin_dir} "
                            f"({pin_delta:+.2f}) on {outcome_name} ({market_key})"
                        ),
                        details={
                            "us_direction": us_dir,
                            "us_avg_delta": round(us_avg, 2),
                            "us_movers": us_movers,
                            "pinnacle_direction": pin_dir,
                            "pinnacle_delta": round(pin_delta, 2),
                            "bet_direction": pin_dir,
                            "value_books": value_books,
                        },
                    )
                )

        return signals

    @staticmethod
    def _calc_delta(
        market_key: str,
        first: tuple[str, float, float | None],
        last: tuple[str, float, float | None],
    ) -> float:
        if market_key == "h2h":
            return last[1] - first[1]
        if first[2] is not None and last[2] is not None:
            return last[2] - first[2]
        return last[1] - first[1]
