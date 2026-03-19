"""Arbitrage detector: negative cross-book hold = guaranteed profit."""

from __future__ import annotations

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType
from sharp_seeker.engine.hold import (
    _implied_prob,
    collect_market_prices_by_market,
    compute_cross_book_hold,
)

log = structlog.get_logger()


class ArbitrageDetector(BaseDetector):
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo

    async def detect(self, event_id: str, fetched_at: str) -> list[Signal]:
        latest = await self._repo.get_latest_snapshots(event_id)
        if not latest:
            return []

        # Index by (market, outcome) -> {bookmaker: row}
        by_market: dict[tuple[str, str], dict[str, dict]] = {}
        meta: tuple[str, str, str, str] | None = None

        for _row in latest:
            row = dict(_row)
            key = (row["market_key"], row["outcome_name"])
            by_market.setdefault(key, {})[row["bookmaker_key"]] = row
            if meta is None:
                meta = (row["sport_key"], row["home_team"], row["away_team"], row["commence_time"])

        if meta is None:
            return []

        # Find unique markets and their outcome pairs
        markets: dict[str, list[str]] = {}
        for mk, on in by_market:
            markets.setdefault(mk, [])
            if on not in markets[mk]:
                markets[mk].append(on)

        signals: list[Signal] = []

        for market_key, outcomes in markets.items():
            if len(outcomes) < 2:
                continue

            # Use first outcome as side A
            outcome_a = outcomes[0]
            cb_a, cb_b, other = collect_market_prices_by_market(
                by_market, market_key, outcome_a,
            )
            if other is None:
                continue

            cross_hold = compute_cross_book_hold(cb_a, cb_b)
            if cross_hold is None or cross_hold >= 0:
                continue

            # Arb found — identify best book for each side
            books_a = by_market.get((market_key, outcome_a), {})
            books_b = by_market.get((market_key, other), {})

            best_a = min(
                books_a.items(),
                key=lambda item: _implied_prob(item[1]["price"]),
            )
            best_b = min(
                books_b.items(),
                key=lambda item: _implied_prob(item[1]["price"]),
            )

            profit_pct = round(abs(cross_hold) * 100, 2)

            signals.append(
                Signal(
                    signal_type=SignalType.ARBITRAGE,
                    event_id=event_id,
                    sport_key=meta[0],
                    home_team=meta[1],
                    away_team=meta[2],
                    commence_time=meta[3],
                    market_key=market_key,
                    outcome_name=outcome_a,
                    strength=min(1.0, abs(cross_hold) * 10),
                    description=(
                        "Arbitrage: {mk} cross-book hold {hold:.2%} "
                        "({pct:.2f}% profit)".format(
                            mk=market_key,
                            hold=cross_hold,
                            pct=profit_pct,
                        )
                    ),
                    details={
                        "cross_book_hold": round(cross_hold, 4),
                        "profit_pct": profit_pct,
                        "side_a": {
                            "outcome": outcome_a,
                            "bookmaker": best_a[0],
                            "price": best_a[1]["price"],
                            "point": best_a[1].get("point"),
                            "deep_link": best_a[1].get("deep_link"),
                        },
                        "side_b": {
                            "outcome": other,
                            "bookmaker": best_b[0],
                            "price": best_b[1]["price"],
                            "point": best_b[1].get("point"),
                            "deep_link": best_b[1].get("deep_link"),
                        },
                    },
                )
            )

        return signals
