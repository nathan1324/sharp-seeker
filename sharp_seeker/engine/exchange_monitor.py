"""Exchange monitor: track Betfair exchange odds for significant implied probability shifts."""

from __future__ import annotations

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType

log = structlog.get_logger()

BETFAIR_KEY = "betfair_ex_eu"
US_BOOKS = {"draftkings", "fanduel", "betmgm", "caesars", "williamhill_us"}


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

        # Index current US book lines by (market, outcome, bookmaker)
        us_current: dict[tuple[str, str, str], dict] = {}
        meta: tuple[str, str, str] | None = None

        for _row in latest:
            row = dict(_row)
            if meta is None:
                meta = (row["sport_key"], row["home_team"], row["away_team"])
            if row["bookmaker_key"] in US_BOOKS:
                us_current[(row["market_key"], row["outcome_name"], row["bookmaker_key"])] = row

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

            # Find US books that haven't adjusted to the exchange movement
            # "Shortened" means exchange thinks more likely → US books with lower
            # implied prob are offering value (higher payout)
            new_exchange_price = row["price"]
            value_books: list[dict] = []
            for (mk, on, bm_key), us_row in us_current.items():
                if mk != row["market_key"] or on != row["outcome_name"]:
                    continue
                us_prob = american_to_implied_prob(us_row["price"])
                # If exchange shortened (more likely) but US book still has
                # lower implied prob → US book offers better payout (value)
                # If exchange drifted (less likely) but US book still has
                # higher implied prob → US book hasn't adjusted
                if direction == "shortened" and us_prob < new_prob:
                    value_books.append({
                        "bookmaker": bm_key,
                        "current_line": us_row["price"],
                        "implied_prob": round(us_prob, 4),
                    })
                elif direction == "drifted" and us_prob > new_prob:
                    value_books.append({
                        "bookmaker": bm_key,
                        "current_line": us_row["price"],
                        "implied_prob": round(us_prob, 4),
                    })

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
                        "value_books": value_books,
                    },
                )
            )

        return signals
