# MultiDevice Datakit

> 🚧 Work in progress !
> This is an evolving research data pipeline, not a finished product. 
> Structure and scope are actively changing as new devices and features come online.

A Python-based, portable, and customizable pipeline for pulling, standardizing, and storing multi-device research data — combining an
**API scheduler**, an **ETL pipeline**, and a **database deployer** into one lightweight data warehouse.

Built for a multi-site study tracking environmental exposure (Atmotube air quality sensors) and biometric data (Fitbit / Google Health) across rotating device assignments and multiple participants — but designed to be extended to any new device type with its own API and parser.

1. **What does this project do?**

- *Ingests*: Pulls each device's cloud API (Atmotube, Google Health/Fitbit) via threaded per-device requests, rate-limit aware. Scheduling is config-driven (`config/schedule.yml`) — the APScheduler wiring itself (`src/scheduler/`) isn't built yet, see Structure below.
- *Processes*: Standardizes and validates per device type — parsing raw API responses into row-dicts (not DataFrames — see note below) matching each destination table's columns exactly, ready for insert.
- *Stores*: Maintains a PostgreSQL + PostGIS database (via Docker) for raw + processed data, with upserts (`ON CONFLICT ... DO UPDATE`) so re-pulling overlapping date ranges is safe, and device/participant assignment tracking (`config/participants.yml`) to reconcile data across a rotating-device study design.
- *Visualizes*: Provides non-technical abilities to visualize the data — internal-facing DB dashboard via Grafana (planned — service is defined in `docker-compose.yml` but not yet provisioned) and public-facing analytical reports via GitHub Pages (`docs/`) — separate from the automated pipeline.

2. **Why does this exists?**

Built specifically for a small-scale research (sole maintainer, some dozen devices) where heavy ETL frameworks (Meltano, Iceberg) are overkill. It delivers the smallest, most maintainable system that ensures reproducibility and allows easy extension to new device types without modifying core logic.


---

## Data Flow from Multiple Devices

***Extract → Load:***  `load.py`'s `__main__` orchestrates the full run: it calls `extract.extract_all_devices()` to pull raw payloads per device, and `load_raw_data()` populates into `raw.ingests` — returns an `ingest_id` per physical device.

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
│   ├── study_registry.yml         # rotating participant-to-device study design 
│   └── pipeline_schedule.yml      # APScheduler job definitions (NOTE: pipeline_schedule.py itself not yet built)
│~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
│
├── deploy/                    ## DB+VIZ DEPLOYMENT
│   ├── docker-compose.yml          # Postgres+PostGIS + Grafana as services
│   ├── postgres/
│   │   ├── test_db.sh              # throwaway container DB for pipeline test-runs
│   │   └── init/
│   │       ├── 00_extensions.sql   # enables postGIS + btree_gist
│   │       ├── 01_schemas.sql
│   │       ├── 03_study.sql        # study.registry — append/upsert log of participants-to-device (users) assignments
│   │       ├── 02_raw.sql          # raw.ingests / pipeline  — append-only log of raw api payoads (JSONB) and pipeline runs
│   │       ├── 04_atmotube.sql     # atmotube.readings — adds GEOMETRY location col
│   │       ├── 05_fitbit.sql       # fitbit.readings / sleep_sessions  / exercise_sessions / profile
│   │       └── ...                 # coming soon: whatsapp + google maps
│   │
│   │
│   └── grafana/
│       ├── boot.yml    # point Grafana at snapshop/ to load dashboard on boot (allowUIUpdates: true -> drag-n-drop GUI edits)  
│       └── snapshots/  # after GUI dashboard designing, json export + git commit here to save (allows for dashboard configs to survive redeployment)
│
│~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
│                              ## ETL PIPELINE
├── src/
│   ├── main.py                      # "start the scheduler, stay live" entrypoint isn't wired up yet
│   │                              
│   ├── general/
│   │   ├── __init__.py
│   │   ├── study_registry.py        # loads/flattens by src/general/study_registry.py; writes to study.registry
│   │   ├── pipeline_logger.py       # logs one row per device per run; writes to raw.pipeline
│   │   └── db_connect.py            # shared db connection import for src/extract/extract.py + src/load/load.py 

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
│   │       ├── __init__.py          # NOT part of ETL pipeline
│   │       ├── backfill_atmotube.py ##  converts historic CSVs -> extract_raw_data()'s output shape
│   │       ├── find_earliest.py     ##  finds one device's earliest date column/s
│   │       ├── inspect_data.py      ##  dumps one device's full raw API pull
│   │       ├── verify_atmotube.py   ##  onboards devices on Atmo Cloud 
│   │       └── verify_fitbit.py     ##  onboards devices on Google account
│   │
│   │
│   │
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── transform.py            # transform_device_data(): device_type → parser via study_registry;
│   │   │                           ##   calls parser.parse(payload, device_id, timezone) uniformly
│   │   │                           ##   embeds { device_type: { device_id: { "data": { table_name: [ {row}, ... ] } } } }
│   │   ├── parse/
│   │   │   ├── __init__.py
│   │   │   ├── atmotube_parser.py
│   │   │   └── fitbit_parser.py
│   │   │
│   │   ├── register/
│   │   │   ├── __init__.py
│   │   │   ├── atmotube_registry.py##  decalres the standard name, measurement unit, dtype, data category per column
│   │   │   └── fitbit_registry.py  ##  declares the ~15 Fitbit data types into a normalized lookup shape
│   │   │
│   │   └── scripts/                
│   │       ├── __init__.py
│   │       └── test_parser.py      # NOT part of ETL pipeline (test + add new device types with parser specifics)
│   │
│   │
│   │
│   └── load/
│       ├── __init__.py
│       ├── load.py                # load_raw_data() -> raw.ingests (append-only, returns ingest_ids);
│       │                          # load_processed_data() -> fitbit.*/atmotube.*, upserts on each table's UNIQUE key,
│       │                          ##   commits per device_id (one bad device doesn't roll back the rest of the batch)
│       └── scripts/
│           ├── __init__.py         
│           ├── seed_study.py      ##   initalizes the study_registry.yml as referential data table
│           └──test_pipeline.py    ##   NOT part of ETL pipeline (tests full wiring; read deloy/postgres/test_db.sh container for step-by-step guide)
│~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
│
└── docs/                          # GitHub Pages — datasheets and nb reports (+ html render helpers)
    ├── __init__.py
    ├── manual.ipynb
    ├── stats.py
    ├── utils.py
    └── atmotube/
        ├── datasheet.md
        └── report.ipynb
```

**Planned, described above but not yet in the repo:**
- `src/scheduler/` (`scheduler.py`, `jobs.py`) — APScheduler wiring that reads `config/schedule.yml` and calls the E→T→L pipeline on a cadence; `apscheduler` is already a `pyproject.toml` dependency
- `notifications/notify.py` — email/Slack alerting on a failed `study.pipeline_runs` row

# How to dev setup (fresh machine / new teammate):

## 1. Clone the repo

```
git clone <repo-url>
cd multidevice_datakit
```

## 2. Create the conda env — this also installs the package (via the -e .[docs] line inside environment.yml)

```
conda env create -f environment.yml
conda activate multidevice_dataviz
```

## 3. Set up local secrets

```
cp .env.example .env
# → fill in .env with real DB creds, Fitbit/Atmotube client secrets, etc.
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