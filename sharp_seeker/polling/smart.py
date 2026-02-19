"""Smart polling: prioritize events by proximity to game time."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import IntEnum

import structlog

from sharp_seeker.api.schemas import EventOddsSchema

log = structlog.get_logger()


class PollPriority(IntEnum):
    HIGH = 1    # within 2 hours — poll every cycle
    MEDIUM = 2  # within 12 hours — poll every other cycle
    LOW = 4     # beyond 12 hours — poll every 4th cycle


def classify_event(event: EventOddsSchema) -> PollPriority:
    """Classify an event's polling priority based on time until commence."""
    try:
        commence = datetime.fromisoformat(event.commence_time)
    except (ValueError, TypeError):
        return PollPriority.HIGH  # if we can't parse, default to high priority

    now = datetime.now(timezone.utc)
    hours_until = (commence - now).total_seconds() / 3600

    if hours_until <= 2:
        return PollPriority.HIGH
    elif hours_until <= 12:
        return PollPriority.MEDIUM
    else:
        return PollPriority.LOW


def should_poll_event(event: EventOddsSchema, cycle_count: int) -> bool:
    """Determine if an event should be polled on this cycle number."""
    priority = classify_event(event)
    return cycle_count % priority == 0


def filter_events_for_cycle(
    events: list[EventOddsSchema], cycle_count: int
) -> list[EventOddsSchema]:
    """Filter events based on smart polling priority for the current cycle."""
    included = []
    skipped = 0
    for event in events:
        if should_poll_event(event, cycle_count):
            included.append(event)
        else:
            skipped += 1

    if skipped > 0:
        log.info("smart_poll_filtered", included=len(included), skipped=skipped)

    return included
