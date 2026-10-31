#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CONTAINER_NAME="multidevice_test_db"
export DB_NAME="test_pipeline"
export DB_HOST="localhost"
export DB_PORT="5436"
export DB_USER="postgres"
export DB_PASSWORD="test"

# trap "docker stop $CONTAINER_NAME 2>/dev/null || true" EXIT

#previous: docker run -d --rm \
docker run -d \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_DB="$DB_NAME" \
    -e POSTGRES_PASSWORD="$DB_PASSWORD" \
    -v "$SCRIPT_DIR/init:/docker-entrypoint-initdb.d:ro" \
    -p "${DB_PORT}:5432" \
    timescale/timescaledb-ha:pg17.11-ts2.31.0

echo "Waiting for Postgres..."
ready=0
for i in {1..30}; do
    echo "Attempt $i/30:"
    if PGPASSWORD="$DB_PASSWORD" psql \
        -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
        -c "SELECT 1" >/dev/null; then
        echo "Database is ready!"
        ready=1
        break
    fi
    sleep 2
done

if [ "$ready" != 1 ]; then
    echo "❌ Database never became ready"
    echo "--- container status ---"
    docker inspect "$CONTAINER_NAME" --format '{{json .State}}' || true
    echo "--- container logs ---"
    docker logs --timestamps "$CONTAINER_NAME" 2>&1 || true
    exit 1
fi

echo "Seeding study data..."
PYTHONPATH=src python -m load.scripts.seed_registry

echo "Running: $@"
"$@"

read -r -p "DB is running on localhost:5436. Press Enter to tear it down..."
echo "Tearing down..."
docker stop "$CONTAINER_NAME"
# you still have to make sure the container is fully removed after

## ====================================================================
# Test-run the entire pipeline: step-by-step guide (^_^ )
#
# The three testers mirror the pipeline. Each can run alone; test_pipeline chains all three.
#   extract.scripts.test_extract      live API pull         -> saves <ppt_id>_<acc_id>_test.json (no DB)
#   transform.scripts.test_transform  parser contract check -> reads that file                   (no DB)
#   load.scripts.test_pipeline        extract + transform + load into THIS throwaway DB         (needs test_db.sh)
# <ids> below = whatever test_extract takes (see its docstring).

# 1. Make this script executable (once)
# chmod +x ./deploy/postgres/test_db.sh

# 2. Sanity check: catch import errors and typos without spinning up Docker
# python -m compileall -q src

# 3. Extract only: real API pull, saved under src/extract/config/secrets/ (uses credentials and API quota, no DB)
# PYTHONPATH=src python -m extract.scripts.test_extract fitbit6 <ids>
# PYTHONPATH=src python -m extract.scripts.test_extract atmotube2 <ids>

# 4. Transform only: parser contract check on a capture you already have (no DB, no API, rerun as often as you like)
# PYTHONPATH=src python -m transform.scripts.test_transform fitbit6 ppt_01_0201_test.json
# PYTHONPATH=src python -m transform.scripts.test_transform atmotube2 ppt_07_atmo_05_test.json --sample 2

# 5. Full pipeline against the disposable container (this script starts it, seeds it, runs your command, tears it down)
#    5a. API pull + transform + load + idempotency check:
#    PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline fitbit6 <ids>
#    PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline atmotube2 <ids>
#    5b. Reuse existing captures (no API quota, the usual way to iterate on load.py):
#    PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline atmotube2 --captures ppt_07_0105_test.json
#    5c. If a capture can't be matched to a usr by file name, give the usr explicitly:
#    PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline fitbit6 --captures ppt_01_0201_test.json --usr-id 12
#    PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline atmotube2 --captures ppt_07_0105_test.json --usr-id 13
#    NOTE: the usr-id is not the actual usr_id from the db...honestly, this is scuffed what i did was spin up the test_db and then count from here...
#           WARNING study_registry.yml: device 'atmo_01': end_date is null but its last assignment ended 2026-09-18 -- extraction may keep pulling it
#           INFO registry.usr added: ppt_02/atmo_02/0102 [2026-06-15, open)
#          ...and then I tore it down and started over. 

# 6. While the db is running: Open another terminal and check it
# PGPASSWORD=test psql -h localhost -p 5436 -U postgres -d test_pipeline
#   At the test_pipeline=# prompt, try:
# \dn
# \dt raw.*
# \dt readings.*
# \d readings.atmotube2
# SELECT count(*) FROM readings.atmotube2;
# SELECT * FROM readings.atmotube2 LIMIT 10;
#   To inspect the raw ingest record too:
# SELECT * FROM raw.ingests;
#   To go back:
# \q

# 6. Typical loop
#    changed a parser   -> step 4 until green, then 5b
#    changed load.py    -> 5b
#    changed a client   -> step 3, then 5a once at the end

# 7. If the container didn't remove itself (e.g. the terminal was killed mid-run)
#    docker kill multidevice_test_db
# Remove any leftover container before rerunning:
#    docker rm -f multidevice_test_db 2>/dev/null || true
