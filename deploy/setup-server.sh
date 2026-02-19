#!/usr/bin/env bash
# Server setup script for Oracle Cloud Free Tier (Ubuntu ARM)
# Run this once after SSH-ing into your new instance.
set -euo pipefail

echo "=== Updating system ==="
sudo apt-get update && sudo apt-get upgrade -y

echo "=== Installing Docker ==="
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Let current user run docker without sudo
sudo usermod -aG docker "$USER"

echo "=== Docker installed ==="
docker --version
docker compose version

echo ""
echo "=== Done! Log out and back in for docker group to take effect. ==="
echo "Then clone your repo and run: deploy/start.sh"
