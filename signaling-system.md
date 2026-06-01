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
  - **NBA/WNBA totals at cross-book hold >= 0.025** (NBA data-driven; see
    Change Log 2026-04-20. WNBA extended by analogy 2026-05-31, no WNBA data
    yet — revisit at 2026-06-15).
  - All US books agree (no outlier).
  - Book in `pd_excluded_books` (global) or `pd_sport_excluded_books[sport]`.
- **Per-sport overrides:** `pd_sport_ml_prob_overrides`,
  `pd_sport_totals_overrides`, `pd_sport_spread_overrides`,
  `pd_sport_excluded_books`.

### Reverse Line — `sharp_seeker/engine/reverse_line.py`
- **Triggers when:** US consensus (2+ movers) and Pinnacle move in opposite
  directions within the steam window.
- **Suppressions:** skip if < 2 US movers or Pinnacle didn't move.

### Arbitrage — `sharp_seeker/engine/arbitrage.py`
- **Triggers when:** cross-book hold goes negative.
- **Strength:** `min(1.0, abs(cross_hold) * 10)` = `profit% / 10`.
- **Suppressions:** books in `arb_excluded_books` (default `["pinnacle"]`).
  Point arbs only compare books at the same line value. Arbs below
  `arb_min_profit_pct` (default `0.0` = all) are dropped at the detector.
- **Strength-filter exemption:** arbs are NOT subject to the pipeline's
  `min_signal_strength` floor (see Change Log 2026-06-01) — `arb_min_profit_pct`
  is their only volume gate.

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

After the pipeline, `DiscordAlerter.send_signals` applies a final **qualifier
gate**: any non-arb signal with zero qualifiers (no Best Combo, no Best Hour,
no Edge Hold for Rapid) is dropped before send. PD signals route to a dedicated
raw channel (and skip the gate) when `discord_webhook_pinnacle_divergence_<sport>`
is set for that sport — used for data-collection on sports without trusted
combos/hours yet. Pipeline filters above are NOT bypassed.

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
| `pd_excluded_books` | `[]` | Books skipped by PD detector (global) |
| `pd_sport_excluded_books` | `{}` | Per-sport books skipped by PD detector (added to global) |
| `exchange_shift_threshold` | 0.05 | Betfair implied prob shift |
| `arb_excluded_books` | `["pinnacle"]` | Books skipped by arb |
| `arb_min_profit_pct` | `0.0` | Min guaranteed-profit % to alert; arbs exempt from min strength |
| `min_signal_strength` | 0.5 | Global min strength |
| `signal_strength_overrides` | `{}` | Per-type min strength |
| `signal_sport_strength_overrides` | `{}` | Per-type+sport min strength |
| `signal_market_strength_overrides` | `{}` | Per-type+market min strength |
| `max_signal_strength_overrides` | `{}` | Per-type strength cap |
| `signal_quiet_hours` | `{}` | UTC hours to suppress (type or type:sport) |
| `signal_blocklist` | `[]` | Blocked type:market / type:sport:market |
| `signal_best_combos` | `[]` | Promoted type:sport:market combos |
| `signal_best_hours` | `{}` | Promoted hours (MST) per type or type:sport |
| `discord_webhook_pinnacle_divergence_wnba` | — | Dedicated raw WNBA-PD channel; bypasses qualifier gate when set |
| `discord_webhook_pinnacle_divergence_mlb` | — | Dedicated raw MLB-PD channel; bypasses qualifier gate when set |
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

### 2026-06-01 — Exempt arbs from min-strength floor + add profit floor
- **Change:** (1) the pipeline min-strength filter (`sharp_seeker/engine/
  pipeline.py`) now exempts `SignalType.ARBITRAGE`. (2) Added
  `arb_min_profit_pct` (float, default `0.0`) to `sharp_seeker/config.py`;
  `ArbitrageDetector.detect` (`sharp_seeker/engine/arbitrage.py`) drops arbs
  below it. `0.0` = surface every arb (any negative cross-book hold).
- **Why (the bug):** arb `strength = min(1.0, abs(cross_hold) * 10)` =
  `profit% / 10`. The generic `min_signal_strength` floor is `0.5`, and prod had
  NO arb strength override (`signal_strength_overrides` was only
  `{rapid_change:0.65, reverse_line:0.65}`, confirmed in deploy logs). So an arb
  had to clear `strength > 0.5` → **`profit% > 5%`** to alert. Real arbs are
  almost always 0.5–2%, so virtually every genuine arb was silently dropped at
  pipeline stage 1. This is why arbs almost never fired.
- **Effect:** with the exemption + `arb_min_profit_pct=0.0`, every detected arb
  (negative hold) now reaches Discord. Operator chose "let anything through";
  raise `ARB_MIN_PROFIT_PCT` later (e.g. `0.5`) if thin/stale-line arbs are noisy.
- **Untouched (intentional):** `arb_excluded_books=["pinnacle"]`, exact-point
  matching for spread/total arbs, and the live-game drop all still apply.
- **Server action:** none required — defaults take effect on deploy.

### 2026-06-01 — Arbitrage @here + @member mention (always on)
- **Change:** added `discord_arb_mention_here` (bool, default `True`) to
  `sharp_seeker/config.py`. Generalized `DiscordAlerter._steam_mention_payload`
  → `_mention_payload` (`sharp_seeker/alerts/discord.py`): it now fires the
  `"@here <@&MEMBER_ROLE_ID>"` ping (with `allowed_mentions={"parse":
  ["everyone"], "roles": [role_id]}`) for **Steam Move** (when
  `discord_steam_mention_here` is on) and **every Arbitrage** signal (when
  `discord_arb_mention_here` is on). Arb reuses `discord_steam_mention_role_id`
  for the role. Other signal types still never ping.
- **Why:** arbitrage is rare and guaranteed-profit, so every arb is worth
  pulling members' attention to — unlike Steam (gated off by default to bound
  noise), arb pings default ON so they fire immediately on deploy. Requested by
  the operator.
- **Default on:** no server `.env` change needed to enable. Set
  `DISCORD_ARB_MENTION_HERE=false` to silence. Note arb already bypasses the
  zero-qualifier suppression gate, so it always reaches `_send_embed`.

### 2026-05-31 — Restore MLB PD quiet hours: 5-6 AM MST overnight bleed
- **Change:** `SIGNAL_QUIET_HOURS["pinnacle_divergence:baseball_mlb"]` set to
  `[12, 13]` UTC (was `[]`). Suppresses MLB PD signals during the 5 AM and 6 AM
  MST hours only. `.env.example` updated; production `.env` requires the same
  edit. No code change — pipeline already supports the sport-specific key
  (`pipeline.py:188-191`, overrides not merges with global PD hours).
- **Why:** May 2026 PD-by-hour breakdown (`scripts/_counterfactual_may.py`,
  ran 2026-05-31):
    - 5 AM MST: n=80, 44.7% WR, **-17.56u**
    - 6 AM MST: n=71, 40.6% WR, **-21.53u**
    - Combined bleed (n=149): 42.7% WR, **-38.94u** — accounts for the entire
      MLB PD Totals May loss. Every other MST hour was profitable; suppressing
      these two flips MLB PD Totals from -1.76u baseline to +37.18u after.
  Structural explanation: 5-6 AM MST is overnight before Pinnacle has fully
  shaped the MLB market and US books are slow to react — lots of divergence,
  mostly noise.
- **Scope discipline:** chose narrow 5-6 AM suppression rather than promoting a
  full `signal_best_hours` set for MLB. Best-hours selection from May data would
  be in-sample (best-hours picked from the same window evaluated against), and
  the +38u figure for the wider 1-qualifier set carries that overfit risk. The
  bleed-only suppression is the conservative read: "MLB overnight is structurally
  bad" doesn't require trusting any in-sample hour ranking.
- **Server action:** production `.env` must update
  `SIGNAL_QUIET_HOURS["pinnacle_divergence:baseball_mlb"]` from `[]` to `[12, 13]`.
- **Review date:** 2026-06-30. Re-run May-style hour breakdown on June data; if
  5-6 AM bleed persists, keep. If June shows different bleed hours, expand the
  list. After 2-3 more weeks accumulate, consider promoting positive MLB hours
  into `SIGNAL_BEST_HOURS["pinnacle_divergence:baseball_mlb"]` with proper
  forward-window validation.
### 2026-05-31 — Add NBA PD totals to X free-play whitelist (followup)
- **Change:** added `pinnacle_divergence:basketball_nba:totals` to
  `X_FREE_PLAY_COMBOS`. Brings the list to 7 entries.
- **Why:** the combo was overlooked in the 2026-04-25 whitelist trim because
  the NBA high-cross-book-hold suppression (2026-04-20 change) hadn't yet
  proven out — PD NBA totals was mid-tuning. Post-suppression May data
  validates it as the highest-EV combo in the system: **70% WR, +42.9u on
  n=57**, with Elite (2+ qualifier) tier at **79% / +53.5u on n=48**. User
  caught the omission during X-recap deploy review.
- **Scope discipline:** chose only PD NBA totals rather than also adding PD
  NBA spreads (+9.5u, n=23) and Steam NBA totals (+11.1u, n=28) — single
  highest-confidence combo is easier to evaluate during a short 3-week NBA
  Finals window. Other NBA combos can be added later if data supports.
- **Timing:** NBA Finals run ~2026-06-05 to 2026-06-22, so this gives ~3
  weeks of playoff-window volume — short but peak engagement period for
  launching the new recap format. Combo goes dormant for offseason after
  the Finals.
- **Server action:** production `.env` must add
  `"pinnacle_divergence:basketball_nba:totals"` to `X_FREE_PLAY_COMBOS`.
  Config-only — no rebuild needed; `docker compose up -d --force-recreate`
  is sufficient (matches the 2026-05-31 MLB quiet-hours pattern).
- **Review date:** covered by the 2026-07-01 review already scheduled in the
  prior X-recap change log entry — audit X free-play unit totals + follower
  growth then.

### 2026-05-31 — X free-play recap upgrades + summer-season combo whitelist
- **Change A (recap format):** `XPoster.post_daily_recap` and `_format_recap`
  in `sharp_seeker/alerts/x_poster.py` now include per-pick units, daily unit
  total in the header (`📊 Yesterday: 3-2 (+1.4u)`), and a month-to-date
  running total in the footer (`May: 12-9 (+8.7u)`). Recap also now ALWAYS
  posts — zero-free-play days emit an accountability line + MTD instead of
  silently skipping. New helpers: `_compute_units`, `_units_from_row`,
  `_fmt_units`, `_month_start_iso`, `_month_label`.
- **Change B (combo whitelist):** added two combos to `X_FREE_PLAY_COMBOS`
  for summer-season volume:
    - `pinnacle_divergence:baseball_mlb:totals` — MLB PD totals are now
      profitable after the 2026-05-31 5-6 AM quiet-hours fix (May post-fix
      projection +37u on n=211).
    - `pinnacle_divergence:basketball_wnba:totals` — WNBA PD filters were
      loosened to match NBA earlier the same day; no historical data but user
      accepted public-loss risk to grow audience cadence during lean season.
  Combos kept as-is: NBA Steam spreads, NBA Rapid h2h, MLB Steam totals, NHL
  PD totals.
- **Why:** strategic shift discussed in session — NBA Finals end ~June 22,
  Elite NBA PD plays dry up, summer is MLB + WNBA only. Sparse Steam-only X
  free plays were producing too little volume to build audience or sustain a
  daily-beat cadence. Recap upgrades add transparency/accountability layer
  (units shown publicly, posts every day even when nothing fires) that
  matches the broader "build the X content franchise during the lean season"
  strategy.
- **Server action:** production `.env` must update `X_FREE_PLAY_COMBOS` to
  add the two new entries (see `.env.example`). Then `docker compose up -d
  --build` for the recap format code changes.
- **Risk callout:** more posts = more public losses. This is intentional —
  user explicitly said "I'm not afraid to lose in public." The MTD footer is
  the discipline mechanism; if a streak goes bad it shows up immediately
  rather than getting buried in selective recap timing.
- **Review date:** 2026-07-01. Audit X free-play unit totals + follower
  growth. If summer combos are bleeding badly, narrow the whitelist; if
  recap-on-empty-days is producing engagement, expand the content franchise
  with the other post types in the strategy doc (commentary posts, weekly
  threads). Weekly recap (`_format_weekly_recap`) was NOT upgraded in this
  PR — defer until daily-format usage validates the approach.

### 2026-05-31 — Align WNBA PD config with NBA (looser detector + high-hold suppression)
- **Change:** WNBA PD inherits the same per-sport overrides NBA has, plus the
  NBA high-cross-book-hold totals suppression:
    - `SIGNAL_SPORT_STRENGTH_OVERRIDES["pinnacle_divergence:basketball_wnba"] = 0.25`
      (was missing, falling back to global 0.5).
    - `PD_SPORT_TOTALS_OVERRIDES["basketball_wnba"] = 0.5` (was missing,
      falling back to global 1.0 pt).
    - `PD_SPORT_SPREAD_OVERRIDES["basketball_wnba"] = 0.5` (was missing,
      falling back to global 1.0 pt).
    - `pinnacle_divergence.py` high-hold suppression list extended from
      `"basketball_nba"` to `("basketball_nba", "basketball_wnba")` so WNBA
      totals at cross-book hold >= 0.025 are dropped, matching NBA.
  No change to ML threshold (NBA also uses the global 0.03).
- **Why:** May 2026 audit (`signal_results` query, 2026-05-31): WNBA produced
  only 8 graded PD signals over ~3 weeks (vs Steam n=88 in the same window).
  Root cause: WNBA inherits ALL global PD defaults because it had no
  sport-specific overrides. The 0.5 strength floor was the binding constraint
  (all 7 WNBA PD spread signals fired at exactly 0.500 strength — piled at the
  cutoff), with the 1.0 pt totals threshold contributing zero totals signals
  for the month. Pinnacle WNBA price coverage (21k snapshots) is ~half of FD/DK
  (~44k each), so there's a structural ceiling on PD volume regardless of
  filters — but the current settings are tighter than the structural ceiling
  warrants.
- **Decision shape:** chose "match NBA + track forward" over a full backtest
  replay. Replay against the 229k WNBA snapshots was offered (path B) but the
  user opted for the analogy approach: trust NBA-derived settings as the
  starting point, watch what happens, adjust at the 2026-06-15 review. The
  high-hold totals suppression was included by user choice ("yes, extend it")
  even though the NBA bleed pattern hasn't been confirmed on WNBA — protects
  against a potential bleed before we see it.
- **Safety net:** WNBA PD signals continue to route to the dedicated raw-PD
  Discord channel (`discord_webhook_pinnacle_divergence_wnba`), so the
  loosened-filter signals don't reach main channels. Noise increase is
  contained.
- **Server action:** production `.env` must update three values:
    - `PD_SPORT_TOTALS_OVERRIDES` to add `"basketball_wnba": 0.5`
    - `PD_SPORT_SPREAD_OVERRIDES` to add `"basketball_wnba": 0.5`
    - `SIGNAL_SPORT_STRENGTH_OVERRIDES` to add
      `"pinnacle_divergence:basketball_wnba": 0.25`
  Then `docker compose up -d --build` (build needed for the code change in
  `pinnacle_divergence.py`).
- **Review date:** 2026-06-15. Re-evaluate WNBA PD volume + outcomes at the
  scheduled review; if loosened filters produce profitable signals, consider
  promoting combos/hours (with explicit 2U Elite sizing decision). If volume
  is still anemic or unprofitable, reconsider whether the structural Pinnacle
  WNBA coverage ceiling makes WNBA PD a poor fit regardless of filters.
### 2026-05-12 — Daily/weekly recaps for raw-PD channels
- **Change:** `sharp_seeker/analysis/reports.py` now treats the two
  `discord_webhook_pinnacle_divergence_<sport>` webhooks as effective per-sport
  overrides for recap purposes via a new `_effective_webhook_overrides()`
  helper. Both `_send_per_type_reports` (exclusion logic) and
  `_send_override_reports` (iteration) use the merged map. For raw-PD entries,
  the stats/CSV queries flip `sent_only` to `False` because raw-PD signals are
  stored with `qualifier_count=0` (they bypass the qualifier gate) and would
  otherwise be filtered out by the default `sent_only=True`.
- **Why:** under the prior code the MLB and WNBA dedicated raw-PD channels
  received per-signal alerts but never received the daily or weekly recap,
  because the recap loop only iterated `discord_webhook_overrides` and the
  dedicated webhook fields lived elsewhere. Confirmed live on prod 2026-05-12.
- **Double-counting note:** the per-type PD recap now excludes `baseball_mlb`
  and `basketball_wnba` so a future qualified PD signal in those sports won't
  appear in both the main PD recap and the raw-PD recap. Today both sports
  have empty `signal_best_combos` / `signal_best_hours`, so the practical
  impact starts at zero and only matters if those configs change.

### 2026-05-11 — Steam Move @here + @member mention
- **Change:** added `discord_steam_mention_here` (bool, default `False`) and
  `discord_steam_mention_role_id` (str, default `"944472531631472640"`) to
  `sharp_seeker/config.py`. When the flag is on, `DiscordAlerter._send_embed`
  (`sharp_seeker/alerts/discord.py`) sets the webhook `content` to
  `"@here <@&MEMBER_ROLE_ID>"` with `allowed_mentions={"parse": ["everyone"],
  "roles": [role_id]}` so the ping actually fires (Discord otherwise renders
  the text but suppresses the notification). Other signal types are unaffected.
- **Why:** Steam is the most-trusted signal type and fires at a manageable
  cadence — worth pulling online viewers' attention to. PD/Rapid/RevLine
  intentionally excluded to keep notification noise bounded; revisit if other
  types warrant it.
- **Default off:** merge can deploy without surprise pings. Flip
  `DISCORD_STEAM_MENTION_HERE=true` on prod when ready.
- **Server action:** production `.env` must set
  `DISCORD_STEAM_MENTION_HERE=true` to enable; role ID override only needed if
  the @member role ID changes in the Sandbox Sports server.

### 2026-05-09 — Dedicated raw PD channels for WNBA + MLB
- **Change:** added two optional config fields
  (`sharp_seeker/config.py`):
  `discord_webhook_pinnacle_divergence_wnba` and
  `discord_webhook_pinnacle_divergence_mlb`. When set,
  `DiscordAlerter.send_signals` (`sharp_seeker/alerts/discord.py`) routes ALL
  PD signals for that sport to the dedicated webhook AND bypasses the discord
  zero-qualifier suppression. Other sports / signal types are unaffected.
  WNBA PD intentionally inherits the global 0.5 strength floor — the existing
  NBA/NHL/MLB 0.25 overrides are data-driven (+22.1u 63% historical) and we
  have no WNBA performance data to justify the same loosening.
- **Also stripped MLB PD quiet hours** —
  `signal_quiet_hours["pinnacle_divergence:baseball_mlb"]` set to `[]` (was
  `[3, 4, 14, 15, 17, 18, 20, 21, 22]` UTC). Reason: MLB PD has produced no
  signals in recent memory under the prior settings, so the original bad-hour
  rationale (8 AM MST 1-4 20% WR, 2 PM MST 0-3 0% WR) is preserved here only
  so it can be restored data-driven if warranted.
- **Why:** WNBA season just opened (added `basketball_wnba` to `SPORTS` same
  day) — no historical data exists to populate combos or sport-specific hours,
  so the qualifier gate would suppress essentially all WNBA PD output. MLB PD
  is similarly suppressed by empty best-hours and missing combos. Routing both
  to a dedicated raw channel (hidden in Discord so noise stays user-only) lets
  the detector flow uncensored for re-evaluation while leaving the main PD
  channel and the qualifier-gate behavior untouched for trusted sports.
- **Server action:** production `.env` must set
  `DISCORD_WEBHOOK_PINNACLE_DIVERGENCE_WNBA=<url>`,
  `DISCORD_WEBHOOK_PINNACLE_DIVERGENCE_MLB=<url>`, and update
  `SIGNAL_QUIET_HOURS["pinnacle_divergence:baseball_mlb"]` to `[]`.
- **Review date:** 2026-06-09. Run `analyze_best_combos_hours.py` against the
  collected WNBA + MLB PD data, promote winners to `SIGNAL_BEST_COMBOS` /
  `SIGNAL_BEST_HOURS`, then unset the dedicated webhooks (or keep as audit
  channels) once the main channel can carry these sports through normal
  qualifiers.

### 2026-04-25 — Per-sport PD book exclusion: DraftKings off for MLB
- **Change:** added `pd_sport_excluded_books: dict[str, list[str]]` config
  (`sharp_seeker/config.py`), merged into the existing `excluded` set in
  `sharp_seeker/engine/pinnacle_divergence.py` alongside the global
  `pd_excluded_books`. Initial value: `{"baseball_mlb": ["draftkings"]}`.
- **Why:** all-time PD-by-book-by-sport breakdown
  (`scripts/analyze_pd_by_book_sport.py`, 2026-04-25):
    - DraftKings on MLB PD: n=155, **25% WR, -86.8u** (Totals only).
    - DraftKings on NBA PD: n=255, 56% WR, +15.6u (profitable).
    - DraftKings on NCAAB PD: n=326, 56% WR, +17.7u (profitable, best).
    - DraftKings on NHL PD: n=211, 59% WR, +12.2u (profitable).
  Adding DK globally would have killed +45.5u of NBA/NCAAB/NHL profit. Per-
  sport scoping recovers the MLB bleed without touching the wins. Estimated
  recovery: +86.8u over the same forward window vs the historical baseline.
- **Server action:** production `.env` must add
  `PD_SPORT_EXCLUDED_BOOKS={"baseball_mlb": ["draftkings"]}` after merge.
- **Review date:** 2026-05-25. Re-run `analyze_pd_by_book_sport.py 30` to
  confirm the bleed is gone, and check whether DK on NBA/NCAAB/NHL PD has
  shifted (don't want to be miss-applying a stale book signal).
- **Follow-up candidates** (not in this change): NHL ML BetRivers (n=32, 9% WR,
  -33.8u), NBA Spread FanDuel (n=101, 39%, -29.2u), NCAAB Total FanDuel (n=109,
  41%, -27.3u). Address in separate PRs after this one deploys.

### 2026-04-25 — X free play policy: Steam spreads only + data-driven combo trim
- **Change:** added a code guard in `sharp_seeker/alerts/x_poster.py` that drops
  any `spreads` signal from free play eligibility unless its type is
  `steam_move`. Trimmed `X_FREE_PLAY_COMBOS` to four data-driven entries:
  `steam_move:basketball_nba:spreads`, `rapid_change:basketball_nba:h2h`,
  `steam_move:baseball_mlb:totals`, `pinnacle_divergence:icehockey_nhl:totals`.
- **Why:** policy decision — spread tweets need cross-book confirmation (Steam),
  not single-book PD divergence. Free-play PD spreads were 33% / -3.8u over the
  last 21d (n=9). Trimmed combos based on a 21-day diagnostic
  (`scripts/diagnose_mlb_volume.py`, `analyze_spread_ml_by_type.py`,
  2026-04-04 → 2026-04-24):
    - Steam NBA spreads: n=45, 61% WR, +7.9u (kept).
    - Rapid NBA ML: n=20, 60% WR, +10.9u (added — best ML bucket).
    - Steam MLB totals: n=40, 69% WR (added — strongest MLB combo by far).
    - PD NHL totals: kept on historical +55.86u; thin 21d sample (n=0 graded
      under "totals" in the script — most NHL PD totals signals were live or
      ungraded). Re-evaluate in 14d.
  Excluded: PD NBA h2h (n=3), PD NCAAB h2h (n=0 in window), Steam ML for any
  sport (`steam_move:h2h` is in `SIGNAL_BLOCKLIST` — would never fire), Rapid
  ML for MLB/NHL (zero signals — MLB runlines don't move 1.0pt and ML 20¢
  threshold rarely trips on tight MLB lines).
- **Server action:** production `.env` on the server must be updated to match
  the new `X_FREE_PLAY_COMBOS` list — `.env.example` is documentation only and
  isn't read by the running container.
- **Review date:** 2026-05-09. Re-run `analyze_spread_ml_by_type.py 14` and
  confirm the four kept combos are still earning their slots; reconsider PD
  NHL totals if the 14-day sample is still thin or negative.
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
