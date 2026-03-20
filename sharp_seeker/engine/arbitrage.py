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

            outcome_a = outcomes[0]

            if market_key == "h2h":
                # H2H: no points — compare all prices directly
                arb = self._check_h2h_arb(
                    by_market, market_key, outcome_a, event_id, meta,
                )
                if arb is not None:
                    signals.append(arb)
            else:
                # Spreads/totals: only compare books at the same point value
                arb = self._check_point_arb(
                    by_market, market_key, outcome_a, outcomes, event_id, meta,
                )
                if arb is not None:
                    signals.append(arb)

        return signals

    @staticmethod
    def _check_h2h_arb(
        by_market: dict, market_key: str, outcome_a: str,
        event_id: str, meta: tuple,
    ) -> Signal | None:
        cb_a, cb_b, other = collect_market_prices_by_market(
            by_market, market_key, outcome_a,
        )
        if other is None:
            return None

        cross_hold = compute_cross_book_hold(cb_a, cb_b)
        if cross_hold is None or cross_hold >= 0:
            return None

        books_a = by_market.get((market_key, outcome_a), {})
        books_b = by_market.get((market_key, other), {})

        best_a = min(books_a.items(), key=lambda item: _implied_prob(item[1]["price"]))
        best_b = min(books_b.items(), key=lambda item: _implied_prob(item[1]["price"]))

        profit_pct = round(abs(cross_hold) * 100, 2)
        return Signal(
            signal_type=SignalType.ARBITRAGE,
            event_id=event_id,
            sport_key=meta[0],
            home_team=meta[1],
            away_team=meta[2],
            commence_time=meta[3],
            market_key=market_key,
            outcome_name=outcome_a,
            strength=min(1.0, abs(cross_hold) * 10),
            description="Arbitrage: {mk} cross-book hold {hold:.2%} ({pct:.2f}% profit)".format(
                mk=market_key, hold=cross_hold, pct=profit_pct,
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

    @staticmethod
    def _check_point_arb(
        by_market: dict, market_key: str, outcome_a: str,
        outcomes: list[str], event_id: str, meta: tuple,
    ) -> Signal | None:
        """Check spreads/totals for arbs, only comparing books at the same point."""
        # Find the other outcome
        other = None
        for on in outcomes:
            if on != outcome_a:
                other = on
                break
        if other is None:
            return None

        books_a = by_market.get((market_key, outcome_a), {})
        books_b = by_market.get((market_key, other), {})

        # Group side A books by point value
        points_a: dict[float, list[tuple[str, dict]]] = {}
        for bm, row in books_a.items():
            pt = row.get("point")
            if pt is not None:
                points_a.setdefault(pt, []).append((bm, row))

        # For each point on side A, find side B books at the complementary point
        # Spreads: side A at -X pairs with side B at +X (same magnitude)
        # Totals: both sides share the same point value
        best_arb: Signal | None = None

        for pt_a, a_entries in points_a.items():
            if market_key == "totals":
                pt_b = pt_a  # Over 6.5 pairs with Under 6.5
            else:
                pt_b = -pt_a  # Team A -1.5 pairs with Team B +1.5

            b_entries = [
                (bm, row) for bm, row in books_b.items()
                if row.get("point") == pt_b
            ]
            if not b_entries:
                continue

            prices_a = [row["price"] for _, row in a_entries]
            prices_b = [row["price"] for _, row in b_entries]

            cross_hold = compute_cross_book_hold(prices_a, prices_b)
            if cross_hold is None or cross_hold >= 0:
                continue

            best_a = min(a_entries, key=lambda item: _implied_prob(item[1]["price"]))
            best_b = min(b_entries, key=lambda item: _implied_prob(item[1]["price"]))
            profit_pct = round(abs(cross_hold) * 100, 2)

            sig = Signal(
                signal_type=SignalType.ARBITRAGE,
                event_id=event_id,
                sport_key=meta[0],
                home_team=meta[1],
                away_team=meta[2],
                commence_time=meta[3],
                market_key=market_key,
                outcome_name=outcome_a,
                strength=min(1.0, abs(cross_hold) * 10),
                description="Arbitrage: {mk} cross-book hold {hold:.2%} ({pct:.2f}% profit)".format(
                    mk=market_key, hold=cross_hold, pct=profit_pct,
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
            # Keep the best arb (most negative hold)
            if best_arb is None or cross_hold < best_arb.details["cross_book_hold"]:
                best_arb = sig

        return best_arb
