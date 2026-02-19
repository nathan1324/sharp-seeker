"""Color and label mappings for alert types."""

from __future__ import annotations

from sharp_seeker.engine.base import SignalType

# Discord embed colors (decimal)
SIGNAL_COLORS: dict[SignalType, int] = {
    SignalType.STEAM_MOVE: 0xFF4500,       # orange-red
    SignalType.RAPID_CHANGE: 0xFFD700,     # gold
    SignalType.PINNACLE_DIVERGENCE: 0x4169E1,  # blue
    SignalType.REVERSE_LINE: 0x8A2BE2,     # violet
    SignalType.EXCHANGE_SHIFT: 0x2ECC71,   # green
}

SIGNAL_LABELS: dict[SignalType, str] = {
    SignalType.STEAM_MOVE: "Steam Move",
    SignalType.RAPID_CHANGE: "Rapid Line Change",
    SignalType.PINNACLE_DIVERGENCE: "Pinnacle Divergence",
    SignalType.REVERSE_LINE: "Reverse Line Movement",
    SignalType.EXCHANGE_SHIFT: "Exchange Shift",
}
