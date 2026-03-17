"""Shared hold (vig/overround) calculations for signal detectors."""

from __future__ import annotations

# Hold boost thresholds — based on PD hold analysis (2026-03-16).
# Lower hold correlates with higher win rate: totals 64% at <4.5% vs 57%
# at 4.5-5.5%; NHL 61% vs 57%.
HOLD_SHARP_THRESHOLD = 0.045   # below 4.5% = sharp book pricing
HOLD_AVERAGE_THRESHOLD = 0.050  # below 5.0% = slightly below median
HOLD_SHARP_BOOST = 0.08        # strength boost for sharp hold
HOLD_AVERAGE_BOOST = 0.04      # strength boost for below-average hold


def _implied_prob(price: float) -> float:
    """Convert American odds to implied probability."""
    if price >= 100:
        return 100.0 / (price + 100.0)
    return abs(price) / (abs(price) + 100.0)


def compute_hold(price_a: float, price_b: float) -> float:
    """Calculate hold (overround) from American odds on both sides.

    Returns a fraction, e.g. 0.048 = 4.8% hold.
    """
    return _implied_prob(price_a) + _implied_prob(price_b) - 1.0


def compute_hold_boost(hold: float | None) -> float:
    """Return strength boost based on hold. Lower hold = bigger boost."""
    if hold is None:
        return 0.0
    if hold < HOLD_SHARP_THRESHOLD:
        return HOLD_SHARP_BOOST
    if hold < HOLD_AVERAGE_THRESHOLD:
        return HOLD_AVERAGE_BOOST
    return 0.0


def compute_hold_for_book(
    current_lines: dict[tuple[str, str, str], dict],
    market_key: str,
    outcome_name: str,
    bookmaker_key: str,
) -> float | None:
    """Compute hold for a bookmaker using the current_lines structure.

    current_lines is keyed by (market_key, outcome_name, bookmaker_key).
    Returns None if the opposite side is not available.
    """
    # Find opposite outcome
    if market_key == "totals":
        other = "Under" if outcome_name == "Over" else "Over"
    else:
        other = None
        for (mk, on, _bm) in current_lines:
            if mk == market_key and on != outcome_name:
                other = on
                break
        if other is None:
            return None

    this_side = current_lines.get((market_key, outcome_name, bookmaker_key))
    other_side = current_lines.get((market_key, other, bookmaker_key))

    if this_side is None or other_side is None:
        return None

    return compute_hold(this_side["price"], other_side["price"])
