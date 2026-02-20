"""Async client for The Odds API."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx
import structlog

from sharp_seeker.api.schemas import EventOddsSchema, SportSchema
from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository
from sharp_seeker.polling.smart import filter_events_for_cycle

log = structlog.get_logger()


class OddsClient:
    MARKETS = "h2h,spreads,totals"

    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo
        self._client = httpx.AsyncClient(
            base_url=settings.odds_api_base_url,
            timeout=30.0,
        )

    async def close(self) -> None:
        await self._client.aclose()

    # ── Public methods ──────────────────────────────────────────────

    async def get_active_sports(self) -> list[SportSchema]:
        """Fetch active sports (free endpoint, 0 credits)."""
        resp = await self._client.get(
            "/sports", params={"apiKey": self._settings.odds_api_key}
        )
        resp.raise_for_status()
        return [SportSchema(**s) for s in resp.json()]

    async def fetch_odds(self, sport_key: str, fetched_at: str | None = None) -> list[EventOddsSchema]:
        """Fetch odds for a sport and store snapshots. Returns parsed events."""
        params: dict[str, Any] = {
            "apiKey": self._settings.odds_api_key,
            "markets": self.MARKETS,
            "bookmakers": ",".join(self._settings.bookmakers),
            "oddsFormat": "american",
        }
        resp = await self._client.get(f"/sports/{sport_key}/odds", params=params)
        resp.raise_for_status()

        # Track credit usage from response headers
        await self._track_credits(resp, f"/sports/{sport_key}/odds")

        events = [EventOddsSchema(**e) for e in resp.json()]
        now = fetched_at or datetime.now(timezone.utc).isoformat()

        # Flatten into snapshot rows
        rows = []
        for event in events:
            for bm in event.bookmakers:
                for market in bm.markets:
                    for outcome in market.outcomes:
                        rows.append(
                            {
                                "event_id": event.id,
                                "sport_key": event.sport_key,
                                "home_team": event.home_team,
                                "away_team": event.away_team,
                                "commence_time": event.commence_time,
                                "bookmaker_key": bm.key,
                                "market_key": market.key,
                                "outcome_name": outcome.name,
                                "price": outcome.price,
                                "point": outcome.point,
                                "fetched_at": now,
                            }
                        )

        inserted = await self._repo.insert_snapshots(rows)
        log.info(
            "odds_fetched",
            sport=sport_key,
            events=len(events),
            snapshots=inserted,
        )
        return events

    async def fetch_all_sports_odds(
        self, cycle_count: int = 1
    ) -> tuple[str, dict[str, list[EventOddsSchema]]]:
        """Fetch odds for all configured sports with smart polling.

        Returns (fetched_at, results_dict) so callers use the same timestamp.
        """
        active = await self.get_active_sports()
        active_keys = {s.key for s in active if not s.has_outrights}

        fetched_at = datetime.now(timezone.utc).isoformat()
        results: dict[str, list[EventOddsSchema]] = {}
        for sport_key in self._settings.sports:
            if sport_key not in active_keys:
                log.info("sport_not_active", sport=sport_key)
                continue
            try:
                all_events = await self.fetch_odds(sport_key, fetched_at=fetched_at)
                # Smart polling: filter events based on proximity to game time
                results[sport_key] = filter_events_for_cycle(all_events, cycle_count)
            except httpx.HTTPStatusError as exc:
                log.error("odds_fetch_failed", sport=sport_key, status=exc.response.status_code)
        return fetched_at, results

    # ── Internal ────────────────────────────────────────────────────

    async def _track_credits(self, resp: httpx.Response, endpoint: str) -> None:
        remaining = resp.headers.get("x-requests-remaining")
        used = resp.headers.get("x-requests-used")
        if remaining is not None and used is not None:
            credits_remaining = int(remaining)
            credits_used = int(used)
            await self._repo.record_api_usage(endpoint, credits_used, credits_remaining)
            log.info(
                "api_credits",
                endpoint=endpoint,
                used=credits_used,
                remaining=credits_remaining,
            )
