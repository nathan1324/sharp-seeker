# Sharp Seeker

Detects sharp (informed) sports betting activity by monitoring line movements across US sportsbooks and sends real-time alerts to Discord.

## How It Works

Sharp Seeker polls [The Odds API](https://the-odds-api.com) on an interval, stores odds snapshots in SQLite, and runs 5 detection strategies to identify sharp money:

| Detector | What It Catches |
|----------|----------------|
| **Steam Move** | 3+ books move the same line in the same direction within 30 min |
| **Rapid Change** | A single book moves a spread by 0.5+ pts or moneyline by 20+ cents |
| **Pinnacle Divergence** | US book offers better value than Pinnacle (the sharpest global book) |
| **Reverse Line Movement** | US consensus moves opposite to Pinnacle — public vs sharp money |
| **Exchange Monitor** | Betfair exchange implied probability shifts by 5%+ |

Every detector also identifies **value bets** — sportsbooks that haven't adjusted to the detected movement yet, showing the outcome and current odds you can still bet at.

When a signal is detected, a color-coded Discord embed is sent with the matchup, market, line movement, strength bar, book-level details, and actionable value bets.

### Auto-Grading

Sharp Seeker automatically grades every signal against final game scores. Each morning it fetches completed scores from The Odds API and resolves each signal as **won**, **lost**, or **push**:

- **Moneyline (H2H)** — Did the picked team win?
- **Spreads** — Did the team cover the spread at signal time?
- **Totals** — Did the over/under hit against the line at signal time?

Grading runs at 14:00 UTC (7 AM MT), followed by daily performance reports at 15:00 UTC (8 AM MT). Each signal type gets its own per-channel report with W/L/P record, individual outcomes, and a market-type breakdown (moneyline/spreads/totals), plus a combined summary goes to the default channel with overall stats by detector and by market.

### Signal Pipeline

Raw signals pass through a 3-stage filter before alerting:

1. **Strength filter** — drops signals below a configurable threshold (default 0.5)
2. **Market-side dedup** — when both sides of a spread/total fire (e.g., Team A -7.5 and Team B +7.5), keeps only the actionable side using signal-type-aware logic (follows Pinnacle direction for RLM, sharp money direction for steam moves, etc.)
3. **Cooldown dedup** — suppresses repeat alerts for the same (event, signal, market, outcome) within 60 minutes

## Project Structure

```
sharp_seeker/
├── main.py                      # Entry point, scheduler setup
├── config.py                    # Pydantic settings from .env
├── cli.py                       # CLI tools (backtest, stats, reports, resolve)
├── db/
│   ├── models.py                # SQLite schema (5 tables)
│   ├── repository.py            # Data access layer
│   └── migrations.py            # Schema creation
├── api/
│   ├── odds_client.py           # The Odds API client + credit tracking + scores
│   └── schemas.py               # Pydantic models for API responses
├── engine/
│   ├── base.py                  # Signal dataclass + BaseDetector ABC
│   ├── steam_move.py            # Steam move detector
│   ├── rapid_change.py          # Rapid change detector
│   ├── pinnacle_divergence.py   # Pinnacle divergence detector
│   ├── reverse_line.py          # Reverse line movement detector
│   ├── exchange_monitor.py      # Exchange monitor detector
│   └── pipeline.py              # Orchestrator + deduplication
├── alerts/
│   ├── discord.py               # Webhook formatting + sending
│   └── models.py                # Alert color/label mappings
├── polling/
│   ├── scheduler.py             # APScheduler config + poll loop
│   ├── budget.py                # API credit tracking + daily summary
│   └── smart.py                 # Priority polling by game proximity
├── analysis/
│   ├── backtest.py              # Replay historical snapshots through detectors
│   ├── grader.py                # Auto-grade signals against final scores
│   ├── performance.py           # Signal win/loss/push tracking
│   └── reports.py               # Daily/weekly Discord reports (per-type + combined)
└── deploy/
    ├── setup-server.sh          # Server provisioning (Docker install)
    ├── start.sh                 # Build and start container
    └── update.sh                # Pull latest code and restart
```

## Quick Start

### Prerequisites

- Python 3.11+
- [The Odds API](https://the-odds-api.com) key
- Discord webhook URL

### Local Development

```bash
# Clone and install
git clone https://github.com/nathan1324/sharp-seeker.git
cd sharp-seeker
pip install -e ".[dev]"

# Configure
cp .env.example .env
# Edit .env with your API key and Discord webhook

# Run
sharp-seeker
```

### Docker

```bash
cp .env.example .env
# Edit .env with your credentials
docker compose up -d
```

### Deploy to Cloud (Oracle Cloud Free Tier)

```bash
# SSH into your instance
ssh -i ~/.ssh/your_key ubuntu@<PUBLIC_IP>

# Clone and setup
git clone https://github.com/nathan1324/sharp-seeker.git
cd sharp-seeker
bash deploy/setup-server.sh

# Log out and back in (docker group permissions)
exit
ssh -i ~/.ssh/your_key ubuntu@<PUBLIC_IP>

# Configure and start
cd sharp-seeker
cp .env.example .env
nano .env  # Fill in credentials
bash deploy/start.sh
```

## Configuration

All settings are configured via `.env` file. See [`.env.example`](.env.example) for all options.

| Variable | Default | Description |
|----------|---------|-------------|
| `ODDS_API_KEY` | — | Your Odds API key (required) |
| `DISCORD_WEBHOOK_URL` | — | Default Discord webhook (required) |
| `DISCORD_WEBHOOK_STEAM_MOVE` | — | Channel for steam move alerts |
| `DISCORD_WEBHOOK_RAPID_CHANGE` | — | Channel for rapid change alerts |
| `DISCORD_WEBHOOK_PINNACLE_DIVERGENCE` | — | Channel for Pinnacle divergence alerts |
| `DISCORD_WEBHOOK_REVERSE_LINE` | — | Channel for reverse line movement alerts |
| `DISCORD_WEBHOOK_EXCHANGE_SHIFT` | — | Channel for exchange shift alerts |
| `ODDS_API_MONTHLY_CREDITS` | `500` | Monthly API credit budget |
| `SPORTS` | `["basketball_nba"]` | Sports to track (JSON array) |
| `BOOKMAKERS` | `["draftkings","fanduel","betmgm","pinnacle"]` | Bookmakers to monitor (JSON array) |
| `POLL_INTERVAL_MINUTES` | `20` | Minutes between polls |
| `QUIET_HOURS_START` | `5` | UTC hour to stop polling |
| `QUIET_HOURS_END` | `14` | UTC hour to resume polling |
| `STEAM_MIN_BOOKS` | `3` | Min books for steam move detection |
| `STEAM_WINDOW_MINUTES` | `30` | Time window for steam moves |
| `RAPID_SPREAD_THRESHOLD` | `0.5` | Min spread change (points) |
| `RAPID_ML_THRESHOLD` | `20` | Min moneyline change (cents) |
| `PINNACLE_SPREAD_THRESHOLD` | `1.0` | Divergence threshold (points) |
| `PINNACLE_ML_PROB_THRESHOLD` | `0.03` | ML divergence threshold (implied prob, 3%) |
| `EXCHANGE_SHIFT_THRESHOLD` | `0.05` | Implied probability shift (5%) |
| `MIN_SIGNAL_STRENGTH` | `0.5` | Min strength to alert (0.0–1.0) |
| `ALERT_COOLDOWN_MINUTES` | `60` | Dedup cooldown per signal |
| `LOG_LEVEL` | `INFO` | Logging level |

### API Credit Usage

Each odds poll costs **3 credits per sport** (using the `bookmakers` parameter instead of `regions` to avoid double-counting). The daily grading job costs **2 credits per sport** to fetch final scores. With the default 1 sport (NBA):

| Action | Credits | Frequency |
|--------|---------|-----------|
| Odds poll | 3/sport | Every 20 min (during active hours) |
| Score fetch (grading) | 2/sport | Once daily at 14:00 UTC |

| Tier | Cost | Credits/mo | Polls (1 sport) | Effective Interval |
|------|------|-----------|-----------------|-------------------|
| Free | $0 | 500 | ~166 | ~every 5 hrs (dev only) |
| 20K | $30/mo | 20,000 | ~6,600 | every 7 min |
| 100K | $59/mo | 100,000 | ~33,000 | every 1-2 min |

With quiet hours enabled (default: midnight–9am ET), polls are skipped overnight, stretching your budget further.

### Adding Sports

Update the `SPORTS` field in `.env`:

```
SPORTS=["basketball_nba","americanfootball_nfl","baseball_mlb"]
```

This multiplies credit usage by the number of sports.

## CLI Tools

```bash
# View signal performance stats
sharp-seeker-tools stats

# Run a backtest over historical data
sharp-seeker-tools backtest 2025-01-15T00:00:00 2025-01-16T00:00:00

# Send a report to Discord
sharp-seeker-tools report daily
sharp-seeker-tools report weekly

# Manually grade unresolved signals against final scores
sharp-seeker-tools resolve
```

In Docker:

```bash
docker compose exec sharp-seeker sharp-seeker-tools stats
docker compose exec sharp-seeker sharp-seeker-tools resolve
```

## Scheduled Jobs

The daemon runs these jobs automatically via APScheduler:

| Job | UTC | Mountain | Purpose |
|-----|-----|----------|---------|
| Odds polling | Every 20 min | — | Fetch odds, detect signals, send alerts |
| **Resolve signals** | **14:00** | **7:00 AM** | Grade yesterday's games against final scores |
| **Daily report** | **15:00** | **8:00 AM** | Per-type + combined performance report |
| **Weekly report** | **Mon 15:30** | **Mon 8:30 AM** | Weekly summary |
| Budget summary | 00:00 | 5:00 PM (prev day) | API credit usage |

Grading runs before the daily report so that results are included. Quiet hours (default 05:00–14:00 UTC) only affect odds polling — reports and grading run regardless.

## Server Management

```bash
# View logs
docker compose logs -f

# Check status
docker compose ps

# Stop
docker compose down

# Update to latest code
bash deploy/update.sh
```

## Running Tests

```bash
pip install -e ".[dev]"
pytest
```

## Tech Stack

- **Python 3.11+** with asyncio
- **SQLite** via aiosqlite
- **APScheduler** for interval/cron-based polling
- **httpx** for async HTTP
- **discord-webhook** for Discord embeds
- **pydantic-settings** for configuration
- **structlog** for structured JSON logging
