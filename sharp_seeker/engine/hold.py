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


def compute_cross_book_hold(
    side_a_prices: list[float],
    side_b_prices: list[float],
) -> float | None:
    """Synthetic hold using best odds from each side across all books.

    Takes a list of American odds prices for each side of a market
    (collected from multiple bookmakers). Returns the hold computed
    from the best (lowest implied probability) price on each side.

    Lower = tighter market consensus. Negative = arbitrage opportunity.
    """
    if not side_a_prices or not side_b_prices:
        return None
    best_a = min(_implied_prob(p) for p in side_a_prices)
    best_b = min(_implied_prob(p) for p in side_b_prices)
    return best_a + best_b - 1.0


def collect_market_prices(
    current_lines: dict[tuple[str, str, str], dict],
    market_key: str,
    outcome_name: str,
) -> tuple[list[float], list[float], str | None]:
    """Collect prices from all books for both sides of a market.

    Returns (side_a_prices, side_b_prices, other_outcome_name).
    Uses the current_lines structure keyed by (market, outcome, book).
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
            return [], [], None

    side_a: list[float] = []
    side_b: list[float] = []
    for (mk, on, _bm), row in current_lines.items():
        if mk != market_key:
            continue
        if on == outcome_name:
            side_a.append(row["price"])
        elif on == other:
            side_b.append(row["price"])

    return side_a, side_b, other


def collect_market_prices_by_market(
    by_market: dict[tuple[str, str], dict[str, dict]],
    market_key: str,
    outcome_name: str,
) -> tuple[list[float], list[float], str | None]:
    """Collect prices from all books for both sides of a market.

    Returns (side_a_prices, side_b_prices, other_outcome_name).
    Uses the by_market structure keyed by (market, outcome) → {book → row}.
    """
    if market_key == "totals":
        other = "Under" if outcome_name == "Over" else "Over"
    else:
        other = None
        for (mk, on) in by_market:
            if mk == market_key and on != outcome_name:
                other = on
                break
        if other is None:
            return [], [], None

    side_a_books = by_market.get((market_key, outcome_name), {})
    side_b_books = by_market.get((market_key, other), {})

    side_a = [row["price"] for row in side_a_books.values()]
    side_b = [row["price"] for row in side_b_books.values()]

    return side_a, side_b, other
