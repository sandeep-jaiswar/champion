#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd)"
cd "$ROOT_DIR"

echo "Starting dev setup for Champion repository"

if ! command -v poetry >/dev/null 2>&1; then
  echo "poetry not found. Install poetry (https://python-poetry.org/docs/#installation) or run:"
  echo "  curl -sSL https://install.python-poetry.org | python3 -"
  exit 1
fi

echo "Installing Python dependencies with poetry..."
poetry install

# Create .env with sensible defaults if missing
ENV_FILE=.env
if [ ! -f "$ENV_FILE" ]; then
  echo "Writing default $ENV_FILE"
  cat > "$ENV_FILE" <<EOF
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=champion_user
CLICKHOUSE_PASSWORD=champion_pass
CLICKHOUSE_DATABASE=champion_market
MLFLOW_TRACKING_URI=http://localhost:5000
EOF
else
  echo "$ENV_FILE already exists — leaving it intact"
fi

STARTED_CONTAINERS=0
if command -v docker >/dev/null 2>&1; then
  # Prefer `docker compose` if available, otherwise fall back to `docker-compose`.
  if docker compose version >/dev/null 2>&1; then
    echo "Starting required docker services with 'docker compose' (clickhouse, mlflow)..."
    docker compose up -d clickhouse mlflow
    STARTED_CONTAINERS=1
  elif command -v docker-compose >/dev/null 2>&1; then
    echo "Starting required docker services with 'docker-compose' (clickhouse, mlflow)..."
    docker-compose up -d clickhouse mlflow
    STARTED_CONTAINERS=1
  else
    echo "Docker installed but neither 'docker compose' nor 'docker-compose' available. Skipping container startup."
  fi
else
  echo "Docker not found. Skipping container startup. Ensure ClickHouse and MLflow are running manually."
fi

if [ "$STARTED_CONTAINERS" -eq 1 ]; then
  echo "Waiting for ClickHouse HTTP (http://localhost:8123) to become available..."
  for i in {1..60}; do
    if curl -sS http://localhost:8123 >/dev/null 2>&1; then
      echo "ClickHouse HTTP available"
      break
    fi
    echo -n "."
    sleep 1
  done
  echo
fi

echo "Running preflight checks..."
poetry run python scripts/preflight_check.py

echo "Dev setup complete. Try: poetry run python -m champion.cli --help"
