#!/usr/bin/env bash
# One-command experiment for Linux / macOS (same as run_all.ps1 on Windows).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results/env results/logs
docker compose build
docker compose up -d
docker compose exec -d client bash -c "bash scripts/run_all.sh >> results/logs/run_all.log 2>&1"
echo "Experiment running in the background. Progress: tail -f results/logs/run_all.log"
