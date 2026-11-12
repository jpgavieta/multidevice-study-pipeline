#!/usr/bin/env bash
# deploy/postgres/test_db.sh -- throwaway Postgres/Timescale for test runs.
#
#   ./deploy/postgres/test_db.sh <command...>        (works from any directory)
#   e.g.  PIPELINE_CATCHUP_HOURS=1 ./deploy/postgres/test_db.sh python src/main.py
#
# It starts a container, waits for it, seeds the registry, runs YOUR command, waits for Enter so you can inspect the DB,
# then removes the container. The exit status is your command's.
#
# SAFETY: the DB_* values exported below WIN over deploy/.env (python-dotenv never overrides a variable that is already
# set), so a test run cannot touch the real database. Everything else in deploy/.env still applies, SLACK_BOT_TOKEN
# included: see the Slack note at the bottom.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"
export PYTHONPATH="$REPO_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"     # callers no longer need to prefix PYTHONPATH=src

CONTAINER_NAME="multidevice_test_db"
export DB_NAME="test_pipeline"
export DB_HOST="localhost"
export DB_PORT="5436"
export DB_USER="postgres"
export DB_PASSWORD="test"

# The container is started WITHOUT --rm so its logs survive a failed start (printed below). Removal is guaranteed here
# instead: on normal exit, on Enter, on Ctrl-C and on any error.
cleanup() {
    echo "Tearing down..."
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# A leftover from a killed run would make `docker run --name` fail.
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

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
    exit 1          # the trap removes the container
fi

echo "Syncing study data..."
python -m load.scripts.seed_registry

echo "Running: $*"
status=0
"$@" || status=$?          # a failing command must not skip the pause and the teardown
echo "Command exited with status $status"

read -r -p "DB is running on localhost:${DB_PORT}. Press Enter to tear it down..." || true
exit "$status"

## ====================================================================
# Test-run the entire pipeline: step-by-step guide (^_^ )
#
# The three testers mirror the pipeline. Each can run alone; test_pipeline chains all three.
#   extract.scripts.test_extract      live API pull         -> saves <ppt_id>_<acc_id>_test.json (no DB)
#   transform.scripts.test_transform  parser contract check -> reads that file                   (no DB)
#   load.scripts.test_pipeline        extract + transform + load into THIS throwaway DB         (needs test_db.sh)
#   src/main.py                       the real scheduler: registry -> forwardfill -> backfill   (needs test_db.sh)
# <ids> below = whatever test_extract takes (see its docstring).

# 1. Make this script executable (once)
# chmod +x ./deploy/postgres/test_db.sh

# 2. Sanity checks that need no Docker and no APIs
# python -m compileall -q src
# PYTHONPATH=src python -m general.settings --check          # config/pipeline_settings.yml + deploy/.env valid?

# 3. Extract only: real API pull, saved under src/extract/config/secrets/ (uses credentials and API quota, no DB)
# PYTHONPATH=src python -m extract.scripts.test_extract fitbit6 <ids>
# PYTHONPATH=src python -m extract.scripts.test_extract atmotube2 <ids>

# 4. Transform only: parser contract check on a capture you already have (no DB, no API, rerun as often as you like)
# PYTHONPATH=src python -m transform.scripts.test_transform fitbit6 ppt_01_0201_test.json
# PYTHONPATH=src python -m transform.scripts.test_transform atmotube2 ppt_07_0105_test.json --sample 2

# 5. Full pipeline against the disposable container (this script starts it, seeds it, runs your command, tears it down)
#    5a. API pull + transform + load + idempotency check:
# ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline fitbit6 <ids>
# ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline atmotube2 <ids>
#    5b. Reuse existing captures (no API quota, the usual way to iterate on load.py):
# ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline atmotube2 --captures ppt_07_0105_test.json
#    5c. If a capture can't be matched to a usr by file name, give the usr explicitly:
# ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline fitbit6 --captures ppt_01_0201_test.json --usr-id 12
# ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline atmotube2 --captures ppt_07_0105_test.json --usr-id 13
#    NOTE: the usr-id is not the actual usr_id from the db...honestly, this is scuffed what i did was spin up the test_db and then count from here...
#           WARNING study_registry.yml: device 'atmo_01': end_date is null but its last assignment ended 2026-09-18 -- extraction may keep pulling it
#           INFO registry.usr added: ppt_02/atmo_02/0102 [2026-06-15, open)
#          ...and then I tore it down and started over.

# 6. The scheduler itself (main.py): catch-up fires a cycle 5 seconds after start, then it waits for the next cron time.
#    Ctrl-C when the log shows "Daily pipeline run finished", then press Enter to tear the DB down.
#    Env vars named in general/settings.py override the yml for this run only:
#    6a. Quick check (small backfill, 30-minute budget instead of 360):
# PIPELINE_CATCHUP_HOURS=1 PIPELINE_BACKFILL_MAX_JOBS=8 PIPELINE_BACKFILL_FIRST_RUN_MINUTES=30 ./deploy/postgres/test_db.sh python src/main.py
#    6b. Full-size run (reads the yml: 40 jobs, 360-minute first-run budget):
# PIPELINE_CATCHUP_HOURS=1 ./deploy/postgres/test_db.sh python src/main.py
#    6c. Backfill alone; run it twice on fresh DBs with --workers 4 and --workers 1 and compare readings.* row counts:
# ./deploy/postgres/test_db.sh python -m load.scripts.backfill_readings --max-jobs 8 --workers 4
#    SLACK: a test run posts to the REAL alert channel (SLACK_BOT_TOKEN comes from deploy/.env). To keep it quiet:
#    SLACK_NOTIFY_ON_OK=0 (only warnings/failures post) or SLACK_ALERT_CHANNEL="#your-test-channel" (the bot must be invited).

# 7. While the db is running: Open another terminal and check it:
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

# 8. Typical loop
#    changed a parser   -> step 4 until green, then 5b
#    changed load.py    -> 5b
#    changed a client   -> step 3, then 5a once at the end
#    changed main.py / settings / backfill -> step 2, then 6a

# 9. If a container is ever left behind (e.g. the machine lost power mid-run)
#    The next run of this script removes it automatically. To do it by hand:
# docker rm -f multidevice_test_db 2>/dev/null || true