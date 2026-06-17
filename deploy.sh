#!/usr/bin/env bash
# Deploy Sharp Seeker to prod.
#
# The container bakes the code into the image (Dockerfile COPYs sharp_seeker/ +
# pip install). Recreating the container alone (`up -d` / `--force-recreate`)
# only reloads .env from the EXISTING image — it does NOT pick up code changes.
# This script always rebuilds, so a code change can't silently ship as old code.
#
# Usage: run from the repo root on the prod host:
#   bash deploy.sh
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f docker-compose.yml ]; then
  echo "ERROR: run this from the repo root (no docker-compose.yml here)" >&2
  exit 1
fi

branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$branch" != "main" ]; then
  echo "==> On '$branch' — switching to main"
  git checkout main
fi

echo "==> Pulling latest main"
git pull --ff-only

echo "==> Deploying commit:"
git log -1 --oneline

echo "==> Rebuilding image + recreating container"
docker compose up -d --build --force-recreate

echo "==> Waiting for the container to come up"
sleep 3

echo "==> Verifying the running container loads its config (proves new code is live)"
docker compose exec -T sharp-seeker python -c "from sharp_seeker.config import Settings; s = Settings(); print('  settings OK | x enabled:', bool(s.x_consumer_key), '| raw combos:', s.x_free_play_raw_combos)"

echo "==> Recent logs"
docker compose logs --tail=20 sharp-seeker

echo
echo "Deploy complete."
