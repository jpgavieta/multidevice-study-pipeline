# MultiDevice Study Pipeline

> 🚧 Work in progress !
> This is an evolving research data pipeline, not a finished product. 
> Structure and scope are actively changing as new devices and features come online.

A Python-based, portable, and customizable pipeline for pulling, standardizing, and storing multi-device research data -- combining an
**API scheduler**, an **ETL pipeline**, and a **database deployer** into one lightweight data warehouse.

Built for a multi-site study tracking environmental exposure (Atmotube air quality sensors) and biometric data (Fitbit / Google Health) across rotating device assignments and multiple participants -- to add a new device type, it must have its own API and parser.

1. **What does this project do?**

- *Ingests*: Pulls each device's cloud API (Atmotube, Google Health/Fitbit) via threaded per-device requests, rate-limit aware. Scheduling runs inside `src/main.py` -- a single daily job (APScheduler `BlockingScheduler` + `CronTrigger`, UTC) wrapping the full extract → load_raw → transform → load_processed sequence, with tenacity retry on run-level failures and Slack alerting via `notify_failure()`. Schedule time is config-driven via `PIPELINE_RUN_HOUR`/`PIPELINE_RUN_MINUTE` in `deploy/.env`.
- *Processes*: Standardizes and validates per device type -- parsing raw API responses into row-dicts matching each destination table's columns exactly, ready for insert.
- *Stores*: Maintains a PostgreSQL + PostGIS database (via Docker) for raw + processed data, with upserts (`ON CONFLICT ... DO UPDATE`) so re-pulling overlapping date ranges is safe, and device/participant assignment tracking (`config/study_registry.yml`) to reconcile data across a rotating-device study design.
- (**TODO**: *Visualizes*: Provides non-technical abilities to visualize the data -- internal-facing DB dashboard via Grafana; service already defined in `docker-compose.yml`)

2. **Why does this exists?**

Built specifically for a small-scale research (sole maintainer, some dozen devices). It delivers the smallest, most maintainable system that ensures reproducibility and allows easy extension to new device types without modifying core logic.


---
## Flow of Data (ELTL)

`extract_all_usrs()` → `load_raw_data()` → `transform_usr_data()` → `load_processed_data()`

1. **Extract:** `extract_all_usrs()` pulls raw payloads from the Atmo Cloud and Google Health APIs for each active usr (participant-device-account assignment).
2. **Load raw:** `load_raw_data()` writes each payload as JSONB to `raw.ingests` and returns an `ingest_id` per usr. A usr whose payload fails to build is skipped and reported, so it doesn't block the rest of the batch.
3. **Transform:** `transform_usr_data()` runs each payload through its device type's parser (`transform/parse/`, registered in `transform/register/`). This produces typed, unit-standardized, UTC-normalized rows.
4. **Load processed:** `load_processed_data()` upserts those rows into `readings.*`, with each row linked back to its `ingest_id`.

Each run logs a row per usr to `raw.pipeline` (via `general/pipeline_logger.py`), so failures show up in the database instead of only in stdout. `load.py` runs the full sequence when invoked directly. In production, `src/main.py` runs it on a schedule.

<iframe allowfullscreen width="560" height="315" src="https://dbdocs.io/embed/187112b888433d4bef3c317f7eaa3199/64acdab7217841d39be0b124287e0977"></iframe>

## How to dev setup (fresh machine / new teammate):

1. **Clone the repo**
```
git clone <repo-url>
cd multidevice-study-pipeline
```

2. **Create the conda env** -- this also installs the package (via the -e .[docs] line inside environment.yml)
```
conda env create -f environment.yml
conda activate multidevice_dataviz
```

3. **Set up local secrets**
cp .env.example .env
# → fill in .env with real DB creds, Fitbit/Atmotube client secrets, Slack bot token, etc.

4. **Deploy the PostgresSQL+Grafana stack**
```
cd deploy
docker compose up
```

5. **Sanity check**: did the package install correctly?
```
python -c "import extract; print(extract.__file__)"
```

6. **Run pipeline, manually once**
```
python -m load.load
```

7. **Run the full pipeline, automatically APScheduler**
```
PYTHONPATH=src python src/main.py
```

Runs once daily at PIPELINE_RUN_HOUR:PIPELINE_RUN_MINUTE UTC (set in deploy/.env). 
For unattended/production use, this is supervised by systemd instead -- see deploy/systemd/multidevice_schedule.service.

For more indepth step-by-step guide to setting up a fresh study project -- see Setting_Up.MD