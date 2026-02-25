"""Rapid change detector: Pinnacle moves a line by a large amount between polls."""

from __future__ import annotations

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType

log = structlog.get_logger()

US_BOOKS = {"draftkings", "fanduel", "betmgm", "caesars", "williamhill_us"}


def _is_steepening(market_key: str, outcome_name: str, old: dict, new: dict) -> bool:
    """Return True if Pinnacle moved the line to make this outcome MORE favored."""
    if market_key == "h2h":
        # More negative price = more favored
        return new["price"] < old["price"]
    if market_key == "totals":
        if outcome_name.lower() == "over":
            return new["point"] > old["point"]  # total went up = sharps on over
        return new["point"] < old["point"]  # total went down = sharps on under
    # Spreads: further from zero = more favored
    return abs(new["point"]) > abs(old["point"])


def _find_other_outcome(
    market_key: str, outcome_name: str, current_lines: dict,
) -> str | None:
    """Find the mirror outcome name for this market (e.g. Team A ↔ Team B, Over ↔ Under)."""
    for mk, on, _ in current_lines:
        if mk == market_key and on != outcome_name:
            return on
    return None


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

        # Index ALL current lines by (market, outcome, bookmaker)
        current_lines: dict[tuple[str, str, str], dict] = {}
        for _row in latest:
            row = dict(_row)
            current_lines[(row["market_key"], row["outcome_name"], row["bookmaker_key"])] = row

        meta: tuple[str, str, str, str] | None = None
        signals: list[Signal] = []

        for _row in latest:
            row = dict(_row)
            key = (row["bookmaker_key"], row["market_key"], row["outcome_name"])
            prev = prev_map.get(key)
            if prev is None:
                continue

            if meta is None:
                meta = (row["sport_key"], row["home_team"], row["away_team"], row["commence_time"])

            market_key = row["market_key"]
            bm = row["bookmaker_key"]

            if market_key == "h2h":
                delta = abs(row["price"] - prev["price"])
                threshold = self._settings.rapid_ml_threshold
            else:
                if row["point"] is not None and prev["point"] is not None:
                    delta = abs(row["point"] - prev["point"])
                    threshold = self._settings.rapid_spread_threshold
                else:
                    continue

            if delta <= threshold:
                continue

            # Only signal when Pinnacle moves — the sharpest book
            if bm != "pinnacle":
                continue

            strength = min(1.0, delta / (threshold * 3))
            steepening = _is_steepening(market_key, row["outcome_name"], prev, row)

            if steepening:
                # Pinnacle steepened this outcome — signal on this side,
                # find US books still at the old (better) line.
                signal_outcome = row["outcome_name"]
                value_books = self._find_stale_books(
                    market_key, signal_outcome, current_lines, prev, row,
                )
            else:
                # Pinnacle shortened this outcome — sharp money is on the
                # other side. Signal the other outcome if US books offer
                # a better price than Pinnacle on that side.
                other_outcome = _find_other_outcome(market_key, row["outcome_name"], current_lines)
                if not other_outcome:
                    continue
                signal_outcome = other_outcome
                pin_other = current_lines.get((market_key, other_outcome, "pinnacle"))
                if not pin_other:
                    continue
                value_books = self._find_better_than_pinnacle(
                    market_key, other_outcome, current_lines, pin_other,
                )
                if not value_books:
                    continue  # no US book beats Pinnacle on the other side

            signals.append(
                Signal(
                    signal_type=SignalType.RAPID_CHANGE,
                    event_id=event_id,
                    sport_key=meta[0],
                    home_team=meta[1],
                    away_team=meta[2],
                    commence_time=meta[3],
                    market_key=market_key,
                    outcome_name=signal_outcome,
                    strength=round(strength, 2),
                    description=(
                        f"Rapid change at {bm}: {row['outcome_name']} "
                        f"({market_key}) delta {delta:.1f}"
                    ),
                    details={
                        "bookmaker": bm,
                        "old_price": prev["price"],
                        "new_price": row["price"],
                        "old_point": prev.get("point"),
                        "new_point": row.get("point"),
                        "delta": round(delta, 2),
                        "value_books": value_books,
                    },
                )
            )

        return signals

    @staticmethod
    def _find_stale_books(
        market_key: str,
        outcome_name: str,
        current_lines: dict,
        prev: dict,
        new: dict,
    ) -> list[dict]:
        """Find US books still at the old (stale) line — value for steepening moves."""
        if market_key == "h2h":
            new_val = new["price"]
            old_val = prev["price"]
        else:
            new_val = new["point"]
            old_val = prev.get("point", prev["price"])

        value_books: list[dict] = []
        for (mk, on, other_bm), other_row in current_lines.items():
            if mk != market_key or on != outcome_name or other_bm not in US_BOOKS:
                continue
            if market_key == "h2h":
                other_val = other_row["price"]
            elif other_row["point"] is not None:
                other_val = other_row["point"]
            else:
                continue
            # Book is "stale" if it's closer to the old line than the new one
            if abs(other_val - old_val) < abs(other_val - new_val):
                value_books.append({
                    "bookmaker": other_bm,
                    "price": other_row["price"],
                    "point": other_row.get("point"),
                    "deep_link": other_row.get("deep_link"),
                })

        value_books.sort(key=lambda vb: _value_sort_key(market_key, outcome_name, vb), reverse=True)
        return value_books

    @staticmethod
    def _find_better_than_pinnacle(
        market_key: str,
        outcome_name: str,
        current_lines: dict,
        pin_row: dict,
    ) -> list[dict]:
        """Find US books offering a better price than Pinnacle on this outcome."""
        value_books: list[dict] = []
        for (mk, on, other_bm), other_row in current_lines.items():
            if mk != market_key or on != outcome_name or other_bm not in US_BOOKS:
                continue
            if market_key == "h2h":
                # Higher (more positive / less negative) price = better for bettor
                if other_row["price"] > pin_row["price"]:
                    value_books.append({
                        "bookmaker": other_bm,
                        "price": other_row["price"],
                        "point": other_row.get("point"),
                        "deep_link": other_row.get("deep_link"),
                    })
            else:
                if other_row["point"] is None or pin_row["point"] is None:
                    continue
                if market_key == "totals" and outcome_name.lower() == "over":
                    # Lower point = easier over = better
                    if other_row["point"] < pin_row["point"]:
                        value_books.append({
                            "bookmaker": other_bm,
                            "price": other_row["price"],
                            "point": other_row["point"],
                            "deep_link": other_row.get("deep_link"),
                        })
                elif market_key == "totals" and outcome_name.lower() == "under":
                    # Higher point = easier under = better
                    if other_row["point"] > pin_row["point"]:
                        value_books.append({
                            "bookmaker": other_bm,
                            "price": other_row["price"],
                            "point": other_row["point"],
                            "deep_link": other_row.get("deep_link"),
                        })
                else:
                    # Spreads: more positive point = better for bettor
                    if other_row["point"] > pin_row["point"]:
                        value_books.append({
                            "bookmaker": other_bm,
                            "price": other_row["price"],
                            "point": other_row["point"],
                            "deep_link": other_row.get("deep_link"),
                        })

        value_books.sort(key=lambda vb: _value_sort_key(market_key, outcome_name, vb), reverse=True)
        return value_books


def _value_sort_key(market_key: str, outcome_name: str, vb: dict) -> float:
    if market_key == "h2h":
        return vb.get("price") or 0
    pt = vb.get("point")
    if pt is None:
        return 0
    if market_key == "totals" and outcome_name.lower() == "over":
        return -pt
    return pt
