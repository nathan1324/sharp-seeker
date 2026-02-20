"""Pinnacle divergence detector: find value where US books offer better odds than Pinnacle."""

from __future__ import annotations

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType
from sharp_seeker.engine.exchange_monitor import american_to_implied_prob

log = structlog.get_logger()

PINNACLE_KEY = "pinnacle"
US_BOOKS = {"draftkings", "fanduel", "betmgm", "caesars", "williamhill_us"}


def _us_has_better_value(
    market_key: str, outcome_name: str, us_value: float, pin_value: float
) -> bool:
    """Check if the US book offers better value to the bettor than Pinnacle.

    - h2h: higher price = better payout (works for both + and - odds)
    - spreads: higher point = better for bettor (less to cover / more points received)
    - totals over: lower point = easier to go over
    - totals under: higher point = easier to stay under
    """
    if market_key == "h2h":
        return us_value > pin_value
    elif market_key == "spreads":
        return us_value > pin_value
    elif market_key == "totals":
        if outcome_name.lower() == "over":
            return us_value < pin_value
        else:
            return us_value > pin_value
    return False


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
                    us_val = row["price"]
                    pin_val = pinnacle["price"]
                    us_prob = american_to_implied_prob(us_val)
                    pin_prob = american_to_implied_prob(pin_val)
                    delta = abs(us_prob - pin_prob)
                    threshold = self._settings.pinnacle_ml_prob_threshold
                else:
                    if row["point"] is not None and pinnacle["point"] is not None:
                        us_val = row["point"]
                        pin_val = pinnacle["point"]
                        delta = abs(us_val - pin_val)
                        threshold = self._settings.pinnacle_spread_threshold
                    else:
                        continue

                if delta < threshold:
                    continue

                # Only alert when US book has BETTER value than Pinnacle
                if not _us_has_better_value(market_key, outcome_name, us_val, pin_val):
                    continue

                strength = min(1.0, delta / (threshold * 3))

                details: dict = {
                    "us_book": bm_key,
                    "us_value": us_val,
                    "pinnacle_value": pin_val,
                    "delta": round(delta, 4 if market_key == "h2h" else 2),
                }
                if market_key == "h2h":
                    details["us_implied_prob"] = round(us_prob, 4)
                    details["pinnacle_implied_prob"] = round(pin_prob, 4)

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
                            f"Value at {bm_key}: {outcome_name} "
                            f"{market_key} better than Pinnacle "
                            f"(delta {delta:.4f})"
                            if market_key == "h2h"
                            else f"Value at {bm_key}: {outcome_name} "
                            f"{market_key} better than Pinnacle "
                            f"(delta {delta:.1f})"
                        ),
                        details=details,
                    )
                )

        return signals
