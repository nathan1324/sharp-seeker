#!/usr/bin/env bash
# Pull latest code and restart Sharp Seeker.
# Usage: ./deploy/update.sh
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== Pulling latest code ==="
git pull origin main

echo "=== Rebuilding and restarting ==="
docker compose up -d --build

echo ""
echo "=== Container status ==="
docker compose ps

echo ""
echo "=== Recent logs ==="
docker compose logs --tail=20
