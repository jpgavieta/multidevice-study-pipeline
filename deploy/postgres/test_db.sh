#!/usr/bin/env bash

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

## ====================================================================
# Test-run Entire Pipeline: Step-by-step Guide (^_^ ) !

# 1. Make test_db.sh executab;e
# chmod +x ./deploy/postgres/test_db.sh

# 2. Quick sanity check without burning a full container cycle (catch import errors, types, etc)
# PYTHONPATH=src python -m py_compile src/general/study_registry.py src/general/db_connect.py src/load/load.py src/load/scripts/seed_study.py src/load/scripts/test_pipeline.py src/transform/scripts/test_parser.py

# 3. Run parser-only test (no DB invovled, confirm transform is healthy independdnet of Docker shi)
# PYTHONPATH=src python -m transform.scripts.test_parser fitbit_01
# PYTHONPATH=src python -m transform.scripts.test_parser atmotube_01

# 4. Run full pipeline test (fitbit data against disposable db container)
# PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline fitbit_01
# PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline atmotube_01

# 5. If the db container doesn't automatically remove itself
# docker kill multidevice_test_db
