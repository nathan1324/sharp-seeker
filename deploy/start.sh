#!/usr/bin/env bash
# Start Sharp Seeker on the server.
# Usage: ./deploy/start.sh
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
    echo "ERROR: .env file not found!"
    echo "Copy .env.example to .env and fill in your credentials:"
    echo "  cp .env.example .env"
    echo "  nano .env"
    exit 1
fi

echo "=== Building and starting Sharp Seeker ==="
docker compose up -d --build

echo ""
echo "=== Container status ==="
docker compose ps

echo ""
echo "=== Tail logs (Ctrl+C to stop watching) ==="
docker compose logs -f
