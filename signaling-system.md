# Signaling System

Reference for Sharp Seeker's signal generation and filtering. **Read this file
before changing combos, hours, thresholds, blocklists, detectors, or filters** to
avoid undoing previous data-driven decisions. After changes, append an entry to
the **Change Log** at the bottom.

## Architecture

Every polling cycle, snapshots are fetched → each event is run through all
detectors → raw signals pass through an 8-stage filter → surviving signals are
persisted to `signal_results` and optionally sent to Discord / X. Only signals
written to `sent_alerts` count as "sent".

---

## Detectors

### Steam Move — `sharp_seeker/engine/steam_move.py`
- **Triggers when:** 3+ US books (`steam_min_books`) move the same line in the
  same direction within `steam_window_minutes` (default 30).
- **Suppressions:**
  - Skip if < `steam_min_books` aligned.
  - Skip if no price dispersion (all books at same line).
- **Per-sport overrides:** none.

### Rapid Change — `sharp_seeker/engine/rapid_change.py`
- **Triggers when:** Pinnacle (only) moves a line by `rapid_spread_threshold`
  (pts) or `rapid_ml_threshold` (cents).
- **Suppressions:** skip if no stale / better-value book to alert on.

### Pinnacle Divergence — `sharp_seeker/engine/pinnacle_divergence.py`
- **Triggers when:** a US book offers better implied odds than Pinnacle, with
  delta over the per-sport threshold (defaults: h2h 3% implied prob, spreads 1.0
  pt, totals 1.0 pt).
- **Suppressions (in order):**
  - delta >= 2.0 for spreads/totals (noise cap).
  - cross-book hold in `[0, 0.02]` (tight market, no edge).
  - **NBA totals at cross-book hold >= 0.025** (data-driven; see Change Log
    2026-04-20).
  - All US books agree (no outlier).
  - Book in `pd_excluded_books`.
- **Per-sport overrides:** `pd_sport_ml_prob_overrides`,
  `pd_sport_totals_overrides`, `pd_sport_spread_overrides`.

### Reverse Line — `sharp_seeker/engine/reverse_line.py`
- **Triggers when:** US consensus (2+ movers) and Pinnacle move in opposite
  directions within the steam window.
- **Suppressions:** skip if < 2 US movers or Pinnacle didn't move.

### Arbitrage — `sharp_seeker/engine/arbitrage.py`
- **Triggers when:** cross-book hold goes negative.
- **Suppressions:** books in `arb_excluded_books` (default `["pinnacle"]`).
  Point arbs only compare books at the same line value.

### Exchange Shift — `sharp_seeker/engine/exchange_monitor.py`
- **Triggers when:** Betfair h2h implied prob shifts by >=
  `exchange_shift_threshold` (default 0.05).
- **Suppressions:** h2h only; requires Betfair present in prev + current.

---

## Filter Pipeline — `sharp_seeker/engine/pipeline.py`

Raw signals pass through 8 stages in order:

| # | Stage | What it does |
|---|---|---|
| 1 | Min Strength | Tiered lookup: `market_strength_overrides` > `sport_strength_overrides` > `strength_overrides` > `min_signal_strength` |
| 2 | Max Strength Cap | Drop trap signals at/above `max_signal_strength_overrides` |
| 3 | Blocklist | Drop `type:market` or `type:sport:market` matches in `signal_blocklist` |
| 4 | Quiet Hours | Drop types at UTC hours in `signal_quiet_hours` (supports `type:sport` keys) |
| 5 | Live Signal | Drop signals for games past `commence_time` |
| 6 | Market-Side Dedup | When both sides fire, keep the more actionable side |
| 7 | Value Books | Require at least one actionable value bet (arb exempt) |
| 8 | Cooldown Dedup | Drop repeat (event, type, market, outcome) within `alert_cooldown_minutes` (default 60) |

---

## Signal-Tuning Config — `sharp_seeker/config.py`

| Setting | Default | Purpose |
|---|---|---|
| `steam_min_books` | 3 | Books needed for steam move |
| `steam_window_minutes` | 30 | Steam alignment window |
| `rapid_spread_threshold` | 1.0 | Min spread move (pts) |
| `rapid_ml_threshold` | 20.0 | Min h2h move (cents) |
| `pinnacle_ml_prob_threshold` | 0.03 | Min h2h PD divergence |
| `pinnacle_spread_threshold` | 1.0 | Min spread PD divergence (pts) |
| `pinnacle_totals_threshold` | 1.0 | Min totals PD divergence (pts) |
| `pd_sport_ml_prob_overrides` | `{}` | Per-sport h2h threshold |
| `pd_sport_totals_overrides` | `{}` | Per-sport totals threshold |
| `pd_sport_spread_overrides` | `{}` | Per-sport spread threshold |
| `pd_excluded_books` | `[]` | Books skipped by PD detector |
| `exchange_shift_threshold` | 0.05 | Betfair implied prob shift |
| `arb_excluded_books` | `["pinnacle"]` | Books skipped by arb |
| `min_signal_strength` | 0.5 | Global min strength |
| `signal_strength_overrides` | `{}` | Per-type min strength |
| `signal_sport_strength_overrides` | `{}` | Per-type+sport min strength |
| `signal_market_strength_overrides` | `{}` | Per-type+market min strength |
| `max_signal_strength_overrides` | `{}` | Per-type strength cap |
| `signal_quiet_hours` | `{}` | UTC hours to suppress (type or type:sport) |
| `signal_blocklist` | `[]` | Blocked type:market / type:sport:market |
| `signal_best_combos` | `[]` | Promoted type:sport:market combos |
| `signal_best_hours` | `{}` | Promoted hours (MST) per type or type:sport |
| `quiet_hours_start` / `quiet_hours_end` | 5 / 14 | UTC hours to skip polling entirely |
| `alert_cooldown_minutes` | 60 | Per (event, type, market, outcome) dedup |
| `x_free_play_combos` | `[]` | Combos eligible to post as X free plays |
| `x_free_play_sport_cap` | 3 | Free plays per sport per day |
| `x_free_play_hourly_cap` | 1 | Free plays per UTC hour |
| `x_max_strength` | 1.0 | Skip PD X tweets at/above this strength |

`.env` requires list/dict values as JSON literals (pydantic-settings).

---

## Change Log

Append a dated entry for every signaling change. Include: what changed, why
(data snapshot, date range, sample size, win%/units/ROI), and file touched.

### 2026-04-20 — Suppress NBA PinDiv totals at cross-book hold >= 2.5%
- **Change:** added in-detector suppression in `pinnacle_divergence.py` for
  `basketball_nba` + `totals` when `cross_book_hold >= 0.025`.
- **Why:** high-hold analysis of sent-only signals (2026-03-19 → 2026-04-19,
  `scripts/analyze_high_hold_sent.py`) showed NBA PinDiv totals at hold >= 2.5%
  were the single largest bleed: 172 signals, 77W-94L (45%), **-56.7u, -33%
  ROI**. Fine-grained tiers degraded monotonically (2.5-3% -8%, 3-4% -28%,
  4-5% -16%). Other NBA markets at high hold stayed profitable (Spread +58% ROI
  on 35, ML +9.5% on 16), so scope is narrow.
- **Kept live:** NBA Steam high (+20.4% ROI, 55 signals), NBA Rapid high
  (+29.1%, 19 signals), NBA Spread PD high (+58.1%, 35), sweet spot 2.0-2.5%
  (186 signals, +6.8%).

### 2026-04-14 — Enable MLB steam totals as best combo (commit `d176dc7`)
- **Change:** added `steam_move:baseball_mlb:totals` to `signal_best_combos`;
  removed MLB steam override from `signal_best_hours` so it inherits the global
  window (9, 11, 13, 14 MST).
- **Why:** 63% win rate, 20W-12L at detection time.

### 2026-03-31 — Lowered MLB PD totals threshold (commit `e3d0093`)
- **Change:** `pd_sport_totals_overrides["baseball_mlb"] = 0.5` (was 1.0).
- **Why:** MLB totals markets are tight; 1.0 threshold was zeroing out signals.

### 2026-03-20 — Raised MLB PD threshold + quiet hours (commit `39003ca`)
- **Change:** removed MLB PD from best combos; added quiet hours for PD and
  rapid change during low-signal periods.
- **Why:** MLB PD was -38u over 4 weeks, 46-51% WR at -110 juice.

### 2026-03-18 — Applied tight-line sport settings to MLB (commit `cf5b36d`)
- **Change:** matched NHL-style tight-market config for MLB (lower thresholds
  where markets cluster).
- **Why:** MLB lines are crowded like NHL; reusing the tight-line settings.

### Historical — PD 0-2% tight-hold suppression
- **Change:** in-detector skip when `0 <= cross_hold <= 0.02` in
  `pinnacle_divergence.py`.
- **Why:** tight hold = market converged, no real edge. 25%, -19.4u in the
  dataset used at the time.
- **Status:** still active.

### Historical — PD spread/total >= 2.0 delta cap
- **Change:** skip PD spread/totals signals with `delta >= 2.0` in
  `pinnacle_divergence.py`.
- **Why:** stable period 49% -10.1u, current period 18% -13.1u at delta 2.0+.
- **Status:** still active.
