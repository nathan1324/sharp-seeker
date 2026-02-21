"""Steam move detector: 3+ books move a line in the same direction within a time window."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

import structlog

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.engine.base import BaseDetector, Signal, SignalType

log = structlog.get_logger()

US_BOOKS = {"draftkings", "fanduel", "betmgm", "caesars", "williamhill_us"}


class SteamMoveDetector(BaseDetector):
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
        meta: dict[str, tuple[str, str, str]] = {}

        for snap in snapshots:
            key = (snap["market_key"], snap["outcome_name"])
            grouped[key][snap["bookmaker_key"]].append(
                (snap["fetched_at"], snap["price"], snap["point"])
            )
            if event_id not in meta:
                meta[event_id] = (snap["sport_key"], snap["home_team"], snap["away_team"])

        sport_key, home, away = meta.get(event_id, ("", "", ""))

        # Build current lines for value book detection
        latest = await self._repo.get_latest_snapshots(event_id)
        current_lines: dict[tuple[str, str, str], dict] = {}
        for _row in latest:
            row = dict(_row)
            mk, on, bm = row["market_key"], row["outcome_name"], row["bookmaker_key"]
            current_lines[(mk, on, bm)] = row

        signals: list[Signal] = []

        for (market_key, outcome_name), book_data in grouped.items():
            # For each book, compute direction of movement (first → last in window)
            moves: list[tuple[str, float]] = []  # (bookmaker, delta)
            for bm_key, entries in book_data.items():
                if len(entries) < 2:
                    continue
                entries.sort(key=lambda e: e[0])
                first = entries[0]
                last = entries[-1]

                if market_key == "h2h":
                    delta = last[1] - first[1]  # price diff
                else:
                    # spreads/totals: use point diff
                    if first[2] is not None and last[2] is not None:
                        delta = last[2] - first[2]
                    else:
                        delta = last[1] - first[1]

                if delta != 0:
                    moves.append((bm_key, delta))

            if len(moves) < self._settings.steam_min_books:
                continue

            # Check if majority move in same direction
            up = [m for m in moves if m[1] > 0]
            down = [m for m in moves if m[1] < 0]

            aligned = max(up, down, key=len)
            if len(aligned) < self._settings.steam_min_books:
                continue

            direction = "up" if aligned is up else "down"
            avg_delta = sum(abs(m[1]) for m in aligned) / len(aligned)
            strength = min(1.0, len(aligned) / max(len(book_data), 1))

            book_details = []
            for bm_key, d in aligned:
                entry: dict = {"bookmaker": bm_key, "delta": round(d, 2)}
                current = current_lines.get((market_key, outcome_name, bm_key))
                if current is not None:
                    entry["price"] = current["price"]
                    entry["point"] = current.get("point")
                book_details.append(entry)

            # Find books that haven't moved yet (stale lines = value bets)
            moved_books = {bm for bm, _ in aligned}
            value_books: list[dict] = []
            for bm_key, entries in book_data.items():
                if bm_key in moved_books or bm_key not in US_BOOKS:
                    continue
                # This book didn't move — still on old line
                current = current_lines.get((market_key, outcome_name, bm_key))
                if current is not None:
                    value_books.append({
                        "bookmaker": bm_key,
                        "price": current["price"],
                        "point": current.get("point"),
                    })

            signals.append(
                Signal(
                    signal_type=SignalType.STEAM_MOVE,
                    event_id=event_id,
                    sport_key=sport_key,
                    home_team=home,
                    away_team=away,
                    market_key=market_key,
                    outcome_name=outcome_name,
                    strength=round(strength, 2),
                    description=(
                        f"Steam move {direction}: {len(aligned)} books moved "
                        f"{outcome_name} ({market_key}) avg {avg_delta:.1f}"
                    ),
                    details={
                        "direction": direction,
                        "books_moved": len(aligned),
                        "avg_delta": round(avg_delta, 2),
                        "book_details": book_details,
                        "value_books": value_books,
                    },
                )
            )

        return signals
