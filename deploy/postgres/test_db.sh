#!/usr/bin/env bash
# make it executable first: chmod +x ./deploy/postgres/test_db.sh
# to end it (also removes): docker stop multidevice_test_db

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CONTAINER_NAME="multidevice_test_db"
export DB_NAME="test_pipeline"
export DB_HOST="localhost"
export DB_PORT="5436"
export DB_USER="postgres"
export DB_PASSWORD="test"

trap "docker stop $CONTAINER_NAME 2>/dev/null || true" EXIT

docker run -d --rm \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_DB="$DB_NAME" \
    -e POSTGRES_PASSWORD="$DB_PASSWORD" \
    -v "$SCRIPT_DIR/init:/docker-entrypoint-initdb.d:ro" \
    -p "${DB_PORT}:5432" postgis/postgis:15-3.3

echo "Waiting for Postgres..."
for i in {1..30}; do
    if PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U postgres -d "$DB_NAME" -c "SELECT 1" >/dev/null 2>&1; then
        echo "Database is ready!"
        break
    fi
    echo "Attempt $i/30: waiting for database..."
    sleep 2
done

echo "Seeding study data..."
PYTHONPATH=src python -m load.scripts.seed_study

echo "Running: $@"
"$@"

echo "Tearing down..."
docker stop "$CONTAINER_NAME"