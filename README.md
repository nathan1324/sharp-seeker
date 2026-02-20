# Sharp Seeker

Detects sharp (informed) sports betting activity by monitoring line movements across US sportsbooks and sends real-time alerts to Discord.

## How It Works

Sharp Seeker polls [The Odds API](https://the-odds-api.com) on an interval, stores odds snapshots in SQLite, and runs 5 detection strategies to identify sharp money:

| Detector | What It Catches |
|----------|----------------|
| **Steam Move** | 3+ books move the same line in the same direction within 30 min |
| **Rapid Change** | A single book moves a spread by 0.5+ pts or moneyline by 20+ cents |
| **Pinnacle Divergence** | US books diverge significantly from Pinnacle (the sharpest global book) |
| **Reverse Line Movement** | US consensus moves opposite to Pinnacle — public vs sharp money |
| **Exchange Monitor** | Betfair exchange implied probability shifts by 5%+ |

When a signal is detected, a color-coded Discord embed is sent with the sport, matchup, market, direction, strength, and book-level details.

## Project Structure

```
sharp_seeker/
├── main.py                      # Entry point, scheduler setup
├── config.py                    # Pydantic settings from .env
├── cli.py                       # CLI tools (backtest, stats, reports)
├── db/
│   ├── models.py                # SQLite schema (4 tables)
│   ├── repository.py            # Data access layer
│   └── migrations.py            # Schema creation
├── api/
│   ├── odds_client.py           # The Odds API client + credit tracking
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
│   ├── performance.py           # Signal win/loss/push tracking
│   └── reports.py               # Daily/weekly Discord reports
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
| `DISCORD_WEBHOOK_URL` | — | Discord webhook URL (required) |
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
| `PINNACLE_ML_THRESHOLD` | `30` | Divergence threshold (cents) |
| `EXCHANGE_SHIFT_THRESHOLD` | `0.05` | Implied probability shift (5%) |
| `ALERT_COOLDOWN_MINUTES` | `60` | Dedup cooldown per signal |
| `LOG_LEVEL` | `INFO` | Logging level |

### API Credit Usage

Credits per poll depend on the number of bookmakers and markets requested per sport. With the default 4 bookmakers and 1 sport, each poll costs ~10-18 credits.

| Tier | Cost | Credits/mo | Approx. Polls |
|------|------|-----------|---------------|
| Free | $0 | 500 | ~28-50 |
| 20K | $30/mo | 20,000 | ~1,100-2,000 |
| 100K | $59/mo | 100,000 | ~5,500-10,000 |

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
```

In Docker:

```bash
docker compose exec sharp-seeker sharp-seeker-tools stats
```

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
