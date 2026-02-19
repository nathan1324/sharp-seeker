"""Base types and ABC for detection strategies."""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from enum import Enum


class SignalType(str, Enum):
    STEAM_MOVE = "steam_move"
    RAPID_CHANGE = "rapid_change"
    PINNACLE_DIVERGENCE = "pinnacle_divergence"
    REVERSE_LINE = "reverse_line"
    EXCHANGE_SHIFT = "exchange_shift"


@dataclass
class Signal:
    signal_type: SignalType
    event_id: str
    sport_key: str
    home_team: str
    away_team: str
    market_key: str
    outcome_name: str
    strength: float  # 0.0–1.0
    description: str
    details: dict = field(default_factory=dict)


class BaseDetector(abc.ABC):
    """Abstract base class for all detection strategies."""

    @abc.abstractmethod
    async def detect(self, event_id: str, fetched_at: str) -> list[Signal]:
        """Analyze snapshots for an event and return any detected signals."""
        ...
