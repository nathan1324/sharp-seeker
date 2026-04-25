"""Pinnacle divergence detector: find value where US books offer better odds than Pinnacle."""

from __future__ import annotations

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType
from sharp_seeker.engine.exchange_monitor import american_to_implied_prob
from sharp_seeker.engine.hold import (
    collect_market_prices_by_market,
    compute_cross_book_hold,
)

log = structlog.get_logger()

PINNACLE_KEY = "pinnacle"
US_BOOKS = {"draftkings", "fanduel", "betmgm", "williamhill_us", "betrivers"}


def _compute_hold(
    by_market: dict[tuple[str, str], dict[str, dict]],
    market_key: str,
    outcome_name: str,
    bookmaker_key: str,
) -> float | None:
    """Compute hold (overround) for a bookmaker on a market.

    hold = implied_prob(side_a) + implied_prob(side_b) - 1.0

    Returns float (e.g. 0.048 = 4.8% hold) or None if opposite side
    is not available in the snapshot data.
    """
    # Determine the opposite outcome
    if market_key == "totals":
        other_outcome = "Under" if outcome_name == "Over" else "Over"
    else:
        # For h2h/spreads, scan by_market for the other outcome
        other_outcome = None
        for (mk, on) in by_market:
            if mk == market_key and on != outcome_name:
                if bookmaker_key in by_market[(mk, on)]:
                    other_outcome = on
                    break
        if other_outcome is None:
            return None

    this_side = by_market.get((market_key, outcome_name), {}).get(bookmaker_key)
    other_side = by_market.get((market_key, other_outcome), {}).get(bookmaker_key)

    if this_side is None or other_side is None:
        return None

    prob_a = american_to_implied_prob(this_side["price"])
    prob_b = american_to_implied_prob(other_side["price"])
    return prob_a + prob_b - 1.0


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
        meta: tuple[str, str, str, str] | None = None

        for _row in latest:
            row = dict(_row)
            key = (row["market_key"], row["outcome_name"])
            by_market.setdefault(key, {})[row["bookmaker_key"]] = row
            if meta is None:
                meta = (row["sport_key"], row["home_team"], row["away_team"], row["commence_time"])

        if meta is None:
            return []

        signals: list[Signal] = []
        sport_excluded = self._settings.pd_sport_excluded_books.get(meta[0], [])
        excluded = set(self._settings.pd_excluded_books) | set(sport_excluded)

        for (market_key, outcome_name), books in by_market.items():
            pinnacle = books.get(PINNACLE_KEY)
            if pinnacle is None:
                continue

            for bm_key, row in books.items():
                if bm_key not in US_BOOKS or bm_key in excluded:
                    continue

                if market_key == "h2h":
                    us_val = row["price"]
                    pin_val = pinnacle["price"]
                    us_prob = american_to_implied_prob(us_val)
                    pin_prob = american_to_implied_prob(pin_val)
                    delta = abs(us_prob - pin_prob)
                    threshold = self._settings.pd_sport_ml_prob_overrides.get(
                        meta[0], self._settings.pinnacle_ml_prob_threshold
                    )
                else:
                    if row["point"] is not None and pinnacle["point"] is not None:
                        us_val = row["point"]
                        pin_val = pinnacle["point"]
                        delta = abs(us_val - pin_val)
                        if market_key == "totals":
                            threshold = self._settings.pd_sport_totals_overrides.get(
                                meta[0], self._settings.pinnacle_totals_threshold
                            )
                        else:
                            threshold = self._settings.pd_sport_spread_overrides.get(
                                meta[0], self._settings.pinnacle_spread_threshold
                            )
                    else:
                        continue

                if delta < threshold:
                    continue

                # Cap large divergences on spreads/totals — consistently noise
                # (stable period: 49% -10.1u, current: 18% -13.1u at delta 2.0+)
                if market_key != "h2h" and delta >= 2.0:
                    continue

                # Only alert when US book has BETTER value than Pinnacle
                if not _us_has_better_value(market_key, outcome_name, us_val, pin_val):
                    continue

                strength = min(1.0, delta / (threshold * 3))

                # Hold metrics: kept for analytics/display, NOT used in strength
                us_hold = _compute_hold(by_market, market_key, outcome_name, bm_key)
                pin_hold = _compute_hold(by_market, market_key, outcome_name, PINNACLE_KEY)

                # Cross-book hold: synthetic hold from best prices across all books
                cb_prices_a, cb_prices_b, _ = collect_market_prices_by_market(
                    by_market, market_key, outcome_name,
                )
                cross_hold = compute_cross_book_hold(cb_prices_a, cb_prices_b)

                # Suppress tight hold — market has converged, no real edge.
                # NBA: block 0-1% only (experiment starting 2026-04-20); we have
                # zero data on the 1-2% band since the blanket 0-2% block was in
                # place. 2-week trial to evaluate. 0-1% remains blocked per the
                # original data (25%, -19.4u at that time).
                # Other sports: keep the original 0-2% block.
                if cross_hold is not None:
                    if meta[0] == "basketball_nba":
                        if 0 <= cross_hold <= 0.01:
                            continue
                    else:
                        if 0 <= cross_hold <= 0.02:
                            continue

                # Suppress NBA totals at high cross-book hold (>= 2.5%) — consistently
                # the largest bleed in sent-signal analysis: 172 signals, 45% win,
                # -56.7u, -33% ROI (range 2026-03-19 to 2026-04-19). Other NBA
                # markets at high hold are profitable, so scope is PD totals only.
                if (
                    market_key == "totals"
                    and meta[0] == "basketball_nba"
                    and cross_hold is not None
                    and cross_hold >= 0.025
                ):
                    continue

                # Price dispersion: how spread out are US books on this side?
                # High dispersion = value book is a real outlier = better signal.
                # Low dispersion = books agree = less reliable.
                same_side_books = by_market.get((market_key, outcome_name), {})
                if market_key == "h2h":
                    us_probs = [
                        american_to_implied_prob(b["price"])
                        for bk, b in same_side_books.items()
                        if bk in US_BOOKS and bk not in excluded
                    ]
                    dispersion = (max(us_probs) - min(us_probs)) if len(us_probs) >= 2 else 0.0
                else:
                    us_points = [
                        b["point"] for bk, b in same_side_books.items()
                        if bk in US_BOOKS and bk not in excluded and b.get("point") is not None
                    ]
                    dispersion = (max(us_points) - min(us_points)) if len(us_points) >= 2 else 0.0

                # Skip signals where all US books agree (no real outlier)
                n_us = len(us_probs if market_key == "h2h" else us_points)
                if dispersion == 0 and n_us >= 2:
                    continue

                details: dict = {
                    "us_book": bm_key,
                    "us_value": us_val,
                    "pinnacle_value": pin_val,
                    "delta": round(delta, 4 if market_key == "h2h" else 2),
                    "us_hold": round(us_hold, 4) if us_hold is not None else None,
                    "pinnacle_hold": round(pin_hold, 4) if pin_hold is not None else None,
                    "cross_book_hold": round(cross_hold, 4) if cross_hold is not None else None,
                    "dispersion": round(dispersion, 4),
                    "hold_boost": 0.0,
                    "value_books": [{
                        "bookmaker": bm_key,
                        "price": row["price"],
                        "point": row.get("point"),
                        "deep_link": row.get("deep_link"),
                    }],
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
                        commence_time=meta[3],
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
