# MultiDevice Study Pipeline

> 🚧 Work in progress !
> This is an evolving research data pipeline, not a finished product. 
> Structure and scope are actively changing as new devices and features come online.

A Python-based, portable, and customizable pipeline for pulling, standardizing, and storing multi-device research data -- combining an
**API scheduler**, an **ETL pipeline**, and a **database deployer** into one lightweight data warehouse.

Built for a multi-site study tracking environmental exposure (Atmotube air quality sensors) and biometric data (Fitbit / Google Health) across rotating device assignments and multiple participants -- but designed to be extended to any new device type with its own API and parser.

1. **What does this project do?**

- *Ingests*: Pulls each device's cloud API (Atmotube, Google Health/Fitbit) via threaded per-device requests, rate-limit aware. Scheduling runs inside `src/main.py` -- a single daily job (APScheduler `BlockingScheduler` + `CronTrigger`, UTC) wrapping the full extract → load_raw → transform → load_processed sequence, with tenacity retry on run-level failures and Slack alerting via `notify_failure()`. Schedule time is config-driven via `PIPELINE_RUN_HOUR`/`PIPELINE_RUN_MINUTE` in `deploy/.env`.
- *Processes*: Standardizes and validates per device type -- parsing raw API responses into row-dicts matching each destination table's columns exactly, ready for insert.
- *Stores*: Maintains a PostgreSQL + PostGIS database (via Docker) for raw + processed data, with upserts (`ON CONFLICT ... DO UPDATE`) so re-pulling overlapping date ranges is safe, and device/participant assignment tracking (`config/study_registry.yml`) to reconcile data across a rotating-device study design.
- [TODO]: *Visualizes*: Provides non-technical abilities to visualize the data -- internal-facing DB dashboard via Grafana (service is defined in `docker-compose.yml` but not yet implemented)

2. **Why does this exists?**

Built specifically for a small-scale research (sole maintainer, some dozen devices) where heavy ETL frameworks (Meltano, Iceberg) are overkill. It delivers the smallest, most maintainable system that ensures reproducibility and allows easy extension to new device types without modifying core logic.


---

## Data Flow from Multiple Devices

***Extract → Load:***  `load.py`'s `__main__` orchestrates the full run: it calls `extract.extract_all_devices()` to pull raw payloads per device, and `load_raw_data()` populates into `raw.ingests` -- returns an `ingest_id` per physical device (plus a `skipped` list for any device whose payload failed to build into a row -- isolated per-device, doesn't block the rest of the batch). In production this same sequence runs on a schedule via `src/main.py` rather than being invoked directly.

***Extract → Transform → Load:*** `transform.transform_device_data()` runs each device's payload through its device type's registered parser (`transform/parse/`, driven by `transform/register/`).`load_processed_data()` to upsert the resulting row-dicts into the destination tables (`fitbit.*` / `atmotube.*`). 

Logs a new row in `raw.pipeline` per device via `general/piepline_logger.py` so failures are visible without reading stdout.

[![Flow of Data from Multiple Devices](dataflow_diagram.svg)](dataflow_diagram.svg)

## Structure of this Repository

```
./
├── README.md
├── multidevice_dataflow.png
│
│                              ## DEV SETTINGS
├── .gitignore
├── pyproject.toml                 # python packaged data building tools
├── environment.yml                # conda environment for python+system-level libraries (not pip installable)
│
├── config/
│   └── study_registry.yml         # rotating participant-to-device study design 
│~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
│
├── deploy/                    ## DB+VIZ DEPLOYMENT
│   ├── docker-compose.yml          # Postgres+PostGIS + Grafana as services
│   ├── .env                        # gitignored service creds (DB, Fitbit/Atmotube secrets, SLACK_BOT_TOKEN + SLACK_ALERT_CHANNEL, PIPELINE_RUN_HOUR/MINUTE)
│   ├── systemd/
│   │   └── multidevice_schedule.service  # unit file (reference copy) -- deployed to /etc/systemd/system/ on the actual host
│   ├── postgres/
│   │   ├── test_db.sh              # throwaway container DB for pipeline test-runs
│   │   └── init/
│   │       ├── 00_extensions.sql   # enables postGIS + btree_gist
│   │       ├── 01_schemas.sql
│   │       ├── 03_study.sql        # study.registry -- append/upsert log of participants-to-device (users) assignments
│   │       ├── 02_raw.sql          # raw.ingests / pipeline  -- append-only log of raw api payoads (JSONB) and pipeline runs
│   │       ├── 04_atmotube.sql     # atmotube.readings -- adds GEOMETRY location col
│   │       ├── 05_fitbit.sql       # fitbit.readings / sleep_sessions  / exercise_sessions / profile
│   │       └── ...                 # coming soon: whatsapp + google maps
│   │
│   │
│   └── grafana/
│       ├── dashboard.yml    # point Grafana at snapshop/ to load dashboard on boot (allowUIUpdates: true -> drag-n-drop GUI edits)  
│       └── dashboard-json/  # after GUI dashboard designing, json export + git commit here to save (allows for dashboard configs to survive redeployment)
│
│~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
│                              ## ETL PIPELINE
├── src/
│   ├── main.py                     # scheduler entrypoint -- ONE daily job (run_daily_pipeline) wrapping
│   │                               #   extract → load_raw → transform → load_processed. 
│   │                               #   APScheduler's BlockingScheduler + CronTrigger (explicit UTC on both), 
│   │                               #   tenacity retry around run-level (not per-device) failures, 
│   │                               #   notify_failure() posts to Slack (chat.postMessage) on a failure that survives retries.
│   │                               #   Started/kept alive by systemd (deploy/systemd/) in production.
│   │                              
│   ├── general/
│   │   ├── __init__.py
│   │   ├── study_registry.py       # loads/flattens by src/general/study_registry.py; writes to study.registry
│   │   ├── pipeline_logger.py      # logs one row per device per run; writes to raw.pipeline
│   │   └── db_connect.py           # shared db connection import for src/extract/extract.py + src/load/load.py 

│   │                                
│   │
│   │   
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── extract.py               # extract_all_devices(): threaded per-device pulls
│   │   │                            ##   embeds {"payload", "ingest_method", "timezone"} per device_id
│   │   │
│   │   ├── clients/                 # API creds per device/source type
│   │   │   ├── __init__.py
│   │   │   ├── atmotube_client.py  
│   │   │   └── fitbit_client.py
│   │   │
│   │   ├── config/
│   │   │   ├── __init__.py
│   │   │   ├── tokens.py            # resolves site -> env var name -> secret
│   │   │   ├── fitbit_tokens.py
│   │   │   ├── atmotube_tokens.py
│   │   │   └── secrets/             # ALWAYS gitignored
│   │   │
│   │   └── scripts/                 
│   │       ├── __init__.py          # NOT part of pipeline
│   │       ├── backfill_atmotube.py ##  converts historic CSVs -> extract_raw_data()'s output shape
│   │       ├── find_earliest.py     ##  finds one device's earliest date column/s
│   │       ├── inspect_data.py      ##  dumps one device's full raw API pull
│   │       ├── verify_atmotube.py   ##  onboards devices on Atmo Cloud 
│   │       └── verify_fitbit.py     ##  onboards devices on Google account 
│   │                                 #    NOTE: must rerun per device to reissue tokens after refresh tokens expire every 7 days; when OAuth app is in Testing mode (TODO: move app to Production mode) 
│   │
│   │
│   │
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── transform.py             # transform_device_data(): device_type → parser via study_registry;
│   │   │                            ##   calls parser.parse(payload, device_id, timezone) uniformly
│   │   │                            ##   embeds { device_type: { device_id: { "data": { table_name: [ {row}, ... ] } } } }
│   │   ├── parse/
│   │   │   ├── __init__.py
│   │   │   ├── atmotube_parser.py
│   │   │   └── fitbit_parser.py
│   │   │
│   │   ├── register/
│   │   │   ├── __init__.py
│   │   │   ├── atmotube_registry.py ##  decalres the standard name, measurement unit, dtype, data category per column
│   │   │   └── fitbit_registry.py   ##  declares the ~15 Fitbit data types into a normalized lookup shape
│   │   │
│   │   └── scripts/                
│   │       ├── __init__.py
│   │       └── test_parser.py       # NOT part of pipeline (test + add new device types with parser specifics)
│   │
│   │
│   │
│   └── load/
│       ├── __init__.py
│       ├── load.py                  # load_raw_data() -> raw.ingests (append-only, returns ingest_ids + fetched_at + skipped);
│       │                            ##   per-device row-building is isolated (a malformed payload is skipped, logged, and excluded from the batch -- doesn't block the rest of the day's devices);
│       │                            ##   the batch INSERT itself is all-or-nothing and re-raises on a genuine DB/connection failure.
│       │                            # load_processed_data() -> fitbit.*/atmotube.*, upserts on each table's UNIQUE key,
│       │                            ##   commits per device_id (one bad device doesn't roll back the rest of the batch)
│       └── scripts/
│           ├── __init__.py         
│           ├── seed_study.py         ##   initalizes the study_registry.yml as referential data table
│           ├── test_pipeline.py      ##   NOT part of pipeline (tests full wiring; read deloy/postgres/test_db.sh container for step-by-step guide)
│           └── test_slack_notify.py  ##   NOT part of pipeline (exercises main.py's notify_failure() Slack delivery in isolation)
│~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
│
└── docs/                          # GitHub Pages -- datasheets and nb reports (+ html render helpers)
    ├── __init__.py
    ├── manual.ipynb
    ├── stats.py
    ├── utils.py
    └── atmotube/
        ├── datasheet.md
        └── report.ipynb
```

**Still planned, not yet in the repo:**
- Persisting `pipeline_run` history/alerting beyond Slack (e.g. a dashboard panel surfacing recent `raw.pipeline` failures directly, rather than only journal/Slack)
- Deciding whether a high per-device skip-rate (not just a run-level exception) should also trigger `notify_failure()` -- currently only a full run-level failure (after tenacity retries are exhausted) triggers a Slack alert; per-device skips/failures are logged to `raw.pipeline` and stdout only

# How to dev setup (fresh machine / new teammate):

## 1. Clone the repo

```
git clone <repo-url>
cd multidevice-study-pipeline
```

## 2. Create the conda env -- this also installs the package (via the -e .[docs] line inside environment.yml)

```
conda env create -f environment.yml
conda activate multidevice_dataviz
```

## 3. Set up local secrets

```
cp .env.example .env
# → fill in .env with real DB creds, Fitbit/Atmotube client secrets, Slack bot token, etc.
```

## 4. Bring up the local Postgres+PostGIS + Grafana stack

```
cd deploy
docker compose up
```

## 5. Sanity check the package installed correctly

```
python -c "import extract; print(extract.__file__)"
```

## 6. Run the pipeline once, manually

```
python -m load.load
```

## 7. Run the full pipeline on its schedule (APScheduler, foreground)

```
PYTHONPATH=src python src/main.py
```

Runs once daily at `PIPELINE_RUN_HOUR`:`PIPELINE_RUN_MINUTE` UTC (set in `deploy/.env`). 
For unattended/production use, this is supervised by systemd instead -- see `deploy/systemd/multidevice_schedule.service`.