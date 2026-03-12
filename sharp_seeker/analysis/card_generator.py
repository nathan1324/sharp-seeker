"""Daily results card image generator for social media."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import structlog
from PIL import Image, ImageDraw, ImageFont

from sharp_seeker.config import Settings
from sharp_seeker.db.repository import Repository

log = structlog.get_logger()

# ── Paths ────────────────────────────────────────────────────────────────────

ASSETS_DIR = Path(__file__).parent.parent.parent / "assets"
FONTS_DIR = ASSETS_DIR / "fonts"
LOGO_PATH = ASSETS_DIR / "logo-square.png"

# ── Colors ───────────────────────────────────────────────────────────────────

BG_COLOR = (15, 27, 61)        # #0f1b3d  dark navy
GOLD = (212, 175, 55)          # #d4af37
WHITE = (255, 255, 255)
GRAY = (160, 170, 200)         # muted blue-gray
GREEN = (0, 200, 83)           # #00c853
RED = (244, 67, 54)            # #f44336

# ── Card sizes ───────────────────────────────────────────────────────────────

SIZES = {
    "1080x1080": (1080, 1080),
    "1080x1920": (1080, 1920),
}


@dataclass
class CardStats:
    yesterday_w: int
    yesterday_l: int
    yesterday_units: float
    month_w: int
    month_l: int
    month_units: float
    ytd_w: int
    ytd_l: int
    ytd_units: float
    streak_count: int
    streak_type: str   # "W" or "L"
    date_str: str      # "March 11, 2026"
    month_name: str    # "March"


class CardGenerator:
    def __init__(self, settings: Settings, repo: Repository) -> None:
        self._settings = settings
        self._repo = repo

    async def generate_daily_cards(self) -> list[str]:
        """Generate daily results card images. Returns list of saved file paths."""
        stats = await self._get_stats()
        if stats is None:
            log.info("card_gen_no_results")
            return []

        output_dir = Path(self._settings.card_output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        paths: list[str] = []

        for label, size in SIZES.items():
            img = self._render_card(size, stats)
            filename = "results_{date}_{label}.png".format(date=today, label=label)
            filepath = output_dir / filename
            img.save(str(filepath), "PNG")
            paths.append(str(filepath))
            log.info("card_saved", path=str(filepath))

        return paths

    async def _get_stats(self) -> CardStats | None:
        """Query free play results and compute stats for card."""
        now = datetime.now(timezone.utc)

        # Yesterday = last 48h to be safe (grading may lag)
        since_yesterday = (now - timedelta(hours=48)).isoformat()
        # Current month = first of this month
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        since_month = month_start.isoformat()
        # YTD = Jan 1 of this year
        ytd_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        since_ytd = ytd_start.isoformat()

        ytd_rows = await self._repo.get_free_play_results_since(since_ytd)
        if not ytd_rows:
            return None

        # Filter to resolved only
        ytd_resolved = [r for r in ytd_rows if dict(r).get("result") is not None]
        if not ytd_resolved:
            return None

        # Yesterday: filter to rows sent in last 48h (already from query)
        yesterday_rows = await self._repo.get_free_play_results_since(since_yesterday)
        yesterday_resolved = [r for r in yesterday_rows if dict(r).get("result") is not None]

        # Month: filter
        month_rows = await self._repo.get_free_play_results_since(since_month)
        month_resolved = [r for r in month_rows if dict(r).get("result") is not None]

        y_w, y_l, y_u = self._tally(yesterday_resolved)
        m_w, m_l, m_u = self._tally(month_resolved)
        ytd_w, ytd_l, ytd_u = self._tally(ytd_resolved)

        streak_count, streak_type = self._compute_streak(ytd_resolved)

        yesterday_date = (now - timedelta(days=1))
        date_str = yesterday_date.strftime("%B %d, %Y")
        month_name = now.strftime("%B")

        return CardStats(
            yesterday_w=y_w,
            yesterday_l=y_l,
            yesterday_units=round(y_u, 2),
            month_w=m_w,
            month_l=m_l,
            month_units=round(m_u, 2),
            ytd_w=ytd_w,
            ytd_l=ytd_l,
            ytd_units=round(ytd_u, 2),
            streak_count=streak_count,
            streak_type=streak_type,
            date_str=date_str,
            month_name=month_name,
        )

    def _tally(self, rows: list) -> tuple[int, int, float]:
        """Count wins, losses, and compute unit profit for a set of results."""
        wins = 0
        losses = 0
        units = 0.0
        for row in rows:
            r = dict(row)
            result = r.get("result")
            if result == "won":
                wins += 1
                units += 1.0  # flat 1u to win
            elif result == "lost":
                losses += 1
                units -= self._risk_amount(r)
            # push = 0u
        return wins, losses, units

    @staticmethod
    def _risk_amount(row_dict: dict) -> float:
        """Calculate risk amount for a 1u-to-win bet from the row's price."""
        details_raw = row_dict.get("details_json")
        if not details_raw:
            return 1.0  # fallback: assume -100 (even money)

        details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        value_books = details.get("value_books", [])
        if not value_books:
            return 1.0

        price = value_books[0].get("price")
        if price is None:
            return 1.0

        return compute_risk(price)

    @staticmethod
    def _compute_streak(rows: list) -> tuple[int, str]:
        """Walk resolved results backwards to find current streak."""
        if not rows:
            return 0, "W"

        # Sort by sent_at descending
        sorted_rows = sorted(rows, key=lambda r: dict(r).get("sent_at", ""), reverse=True)

        streak_type = ""
        streak_count = 0

        for row in sorted_rows:
            result = dict(row).get("result")
            if result == "push":
                continue
            if result not in ("won", "lost"):
                continue

            r_type = "W" if result == "won" else "L"
            if not streak_type:
                streak_type = r_type
                streak_count = 1
            elif r_type == streak_type:
                streak_count += 1
            else:
                break

        return streak_count, streak_type or "W"

    def _render_card(self, size: tuple[int, int], stats: CardStats) -> Image.Image:
        """Render a results card image at the given size."""
        w, h = size
        img = Image.new("RGB", (w, h), BG_COLOR)
        draw = ImageDraw.Draw(img)

        is_story = h > w  # 1080x1920 story format

        # Scale factor relative to 1080 base width
        s = w / 1080.0

        # Load fonts
        font_bold = _load_font("Inter-Bold.ttf", int(28 * s))
        font_title = _load_font("Inter-Bold.ttf", int(36 * s))
        font_hero = _load_font("Inter-Bold.ttf", int(96 * s))
        font_label = _load_font("Inter-Regular.ttf", int(22 * s))
        font_record = _load_font("Inter-Bold.ttf", int(42 * s))
        font_units = _load_font("Inter-Bold.ttf", int(26 * s))
        font_streak = _load_font("Inter-Bold.ttf", int(32 * s))
        font_footer = _load_font("Inter-Regular.ttf", int(20 * s))

        # Vertical layout positions
        if is_story:
            # Story: more vertical space, push content to upper-center
            y_logo = int(180 * s)
            y_brand = y_logo + int(110 * s)
            y_title = y_brand + int(60 * s)
            y_hero = y_title + int(80 * s)
            y_hero_label = y_hero + int(110 * s)
            y_divider = y_hero_label + int(60 * s)
            y_col_label = y_divider + int(40 * s)
            y_col_record = y_col_label + int(40 * s)
            y_col_units = y_col_record + int(55 * s)
            y_streak = y_col_units + int(80 * s)
            y_footer = h - int(120 * s)
        else:
            # Square: tighter layout
            y_logo = int(50 * s)
            y_brand = y_logo + int(100 * s)
            y_title = y_brand + int(50 * s)
            y_hero = y_title + int(70 * s)
            y_hero_label = y_hero + int(105 * s)
            y_divider = y_hero_label + int(50 * s)
            y_col_label = y_divider + int(35 * s)
            y_col_record = y_col_label + int(35 * s)
            y_col_units = y_col_record + int(50 * s)
            y_streak = y_col_units + int(65 * s)
            y_footer = h - int(60 * s)

        cx = w // 2  # center x

        # ── Logo ─────────────────────────────────────────────────────
        logo_size = int(80 * s)
        if LOGO_PATH.exists():
            logo = Image.open(str(LOGO_PATH)).convert("RGBA")
            logo = logo.resize((logo_size, logo_size), Image.LANCZOS)
            logo_x = cx - logo_size // 2
            img.paste(logo, (logo_x, y_logo), logo)

        # ── Brand name ───────────────────────────────────────────────
        _draw_centered(draw, "SANDBOX SPORTS", font_bold, GOLD, cx, y_brand)

        # ── Title ────────────────────────────────────────────────────
        _draw_centered(draw, "FREE PLAY RESULTS", font_title, WHITE, cx, y_title)

        # ── Hero stat: YTD units ─────────────────────────────────────
        sign = "+" if stats.ytd_units >= 0 else ""
        hero_text = "{sign}{units:.1f}u".format(sign=sign, units=stats.ytd_units)
        hero_color = GREEN if stats.ytd_units >= 0 else RED
        _draw_centered(draw, hero_text, font_hero, hero_color, cx, y_hero)

        _draw_centered(draw, "2026 YTD Profit", font_label, GRAY, cx, y_hero_label)

        # ── Gold divider ─────────────────────────────────────────────
        margin = int(80 * s)
        draw.line([(margin, y_divider), (w - margin, y_divider)], fill=GOLD, width=int(2 * s))

        # ── Two columns: Yesterday / Month ───────────────────────────
        col_left = w // 4
        col_right = 3 * w // 4

        # Labels
        _draw_centered(draw, "Yesterday", font_label, GRAY, col_left, y_col_label)
        _draw_centered(draw, stats.month_name, font_label, GRAY, col_right, y_col_label)

        # Records
        y_record = "{w}-{l}".format(w=stats.yesterday_w, l=stats.yesterday_l)
        m_record = "{w}-{l}".format(w=stats.month_w, l=stats.month_l)
        _draw_centered(draw, y_record, font_record, WHITE, col_left, y_col_record)
        _draw_centered(draw, m_record, font_record, WHITE, col_right, y_col_record)

        # Unit numbers
        y_sign = "+" if stats.yesterday_units >= 0 else ""
        m_sign = "+" if stats.month_units >= 0 else ""
        y_units_text = "{s}{u:.1f}u".format(s=y_sign, u=stats.yesterday_units)
        m_units_text = "{s}{u:.1f}u".format(s=m_sign, u=stats.month_units)
        y_color = GREEN if stats.yesterday_units >= 0 else RED
        m_color = GREEN if stats.month_units >= 0 else RED
        _draw_centered(draw, y_units_text, font_units, y_color, col_left, y_col_units)
        _draw_centered(draw, m_units_text, font_units, m_color, col_right, y_col_units)

        # ── Streak ───────────────────────────────────────────────────
        if stats.streak_count >= 3:
            streak_emoji = "\U0001f525" if stats.streak_type == "W" else ""
            streak_text = " {count}{t} Streak".format(
                count=stats.streak_count, t=stats.streak_type,
            )
            # Draw without emoji (Pillow can't render emoji reliably)
            streak_display = "{count}{t} Streak".format(
                count=stats.streak_count, t=stats.streak_type,
            )
            streak_color = GREEN if stats.streak_type == "W" else RED
            _draw_centered(draw, streak_display, font_streak, streak_color, cx, y_streak)

        # ── Footer ───────────────────────────────────────────────────
        _draw_centered(draw, "@SandboxSportsX", font_footer, GRAY, cx, y_footer)

        return img


def compute_risk(price: float) -> float:
    """Compute risk amount for a flat-1u-to-win bet.

    Minus odds (e.g. -110): risk |price|/100 to win 1u.
    Plus odds (e.g. +150): risk 100/price to win 1u.
    """
    if price < 0:
        return abs(price) / 100.0
    elif price > 0:
        return 100.0 / price
    return 1.0  # edge case: even money


def _load_font(name: str, size: int) -> ImageFont.FreeTypeFont:
    """Load a font from the assets/fonts directory, with fallback."""
    font_path = FONTS_DIR / name
    if font_path.exists():
        return ImageFont.truetype(str(font_path), size)
    return ImageFont.load_default()


def _draw_centered(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.FreeTypeFont,
    color: tuple,
    cx: int,
    y: int,
) -> None:
    """Draw text centered horizontally at (cx, y)."""
    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    x = cx - text_w // 2
    draw.text((x, y), text, fill=color, font=font)
