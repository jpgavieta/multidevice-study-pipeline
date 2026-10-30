# Setting Up

This is a step-by-step walkthrough of everythin to do after cloning the repo and installing the environment (see the main [README](./README.md) for `conda env create` / `.env` steps): stand up the database, onboard the devices, seed the study registry, and backfill any pre-existing CSV data. 

>NOTE: This MUST be done in this order -- onboarding needs devices declared in `config/study_registry.yml`, seeding needs the DB up, and backfilling needs the registry already seeded.

All commands below assume you're at the repo root (terminal command: `cd ..`) with the `multidevice_dataviz` conda env active, and that `deploy/.env` is already filled in (DB creds, Fitbit/Atmotube secrets, etc.) per the README's step 3.

---

## 1. Set up the database

The DB is Postgres+PostGIS, run via Docker Compose, with schema/tables created automatically from SQL files on first boot.

### Bring up the real (persistent) stack

```bash
cd deploy
docker compose up -d
```

This starts two services (`deploy/docker-compose.yml`):
- **`app_postgres`** -- Postgres 17 + TimescaleDB + PostGIS (`timescale/timescaledb-ha:pg17`), with `deploy/postgres/init/` mounted as Docker's `docker-entrypoint-initdb.d/`. On a **first boot only** (empty `pg_data` volume), Postgres runs every `.sql` file in that folder, in filename order:

  | File | Creates |
  |---|---|
  | `00_extensions.sql` | `timescaledb`, `postgis`, `btree_gist` extensions |
  | `01_schemas.sql` | `registry`, `raw`, `readings`, `accounts` schemas |
  | `02_registry.sql` | `registry.type` / `site` / `dvc` / `ppt` / `acc` / `usr` (+ views) |
  | `03_raw.sql` | `raw.ingests`, `raw.pipeline`, `raw.pull_coverage` |
  | `04_readings.sql` | `readings.atmotube2` / `fitbit6` (hypertables), `fitbit6_ecg` / `_exercise` / `_sleep` / `_days` (+ dashboard views) |
  | `05_accounts.sql` | `accounts.usr` |

  Order matters: each file only references objects created by earlier ones (schemas → `registry.usr` → `raw.*` → `readings.*` / `accounts.*`, which FK to both).

- **`app_grafana`** -- dashboard UI on port `300`, depends on `postgres`.

Sanity check it came up clean:

```bash
docker logs app_postgres
```

> If you ever need to wipe and re-run the init scripts (e.g. you changed a schema file), you have to drop the volume -- Postgres only runs `docker-entrypoint-initdb.d/` when the data directory is empty:
> ```bash
> docker compose down -v
> docker compose up -d
> ```

### Disposable test DB (optional, for pipeline test-runs)

For a throwaway container instead of the persistent stack -- e.g. while iterating on a parser or testing the full extract→load flow without touching real data -- use `deploy/postgres/test_db.sh` instead. It spins up a temporary container on port `5436`, runs the same init scripts, auto-seeds the study registry, runs whatever command you pass it, then tears the container down:

```bash
chmod +x deploy/postgres/test_db.sh   # once
PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline atmotube_01
```

See the step-by-step guide in the comments at the bottom of that script for more (parser-only checks, syntax sanity checks, etc.).

---

## 2. Onboard the devices

Every device (Fitbit or Atmotube) needs to exist in `config/study_registry.yml`'s `devices:` list **before** you onboard it -- that's where its `device_id`, `type`, `site`, and `access` (Google account for Fitbit / MAC for Atmotube) come from. Add the entry first, then onboard.

### Fitbit / Google Health (OAuth, per-device)

Each physical Fitbit has its own **Google account**, and gets its own OAuth token -- the OAuth *client* (client_id/secret) is shared across all devices, but tokens are per-device (`extract/config/secrets/fitbit/tokens/<device_id>.json`).

**Prerequisites for a brand-new device:**
1. Add the device's Google account as a **Test User** in Google Cloud Console (OAuth consent screen → Audience) -- the OAuth app runs in Testing mode.
2. Know the `device_id` you're assigning it (add it to `config/study_registry.yml` first, e.g. `fitbit_08`).

**Onboard (or just verify) one device:**

```bash
PYTHONPATH=src python -m extract.scripts.verify_fitbit fitbit_08
```

`verify_fitbit.py` handles both onboarding and verification with the same call -- it reuses a valid saved token silently if one exists, and only opens a browser for full OAuth consent if it doesn't. The printed output tells you which one happened. If a browser opens, **sign in to that device's Google account** (not your personal one) and approve the requested scopes (activity/fitness, health metrics, location, sleep, profile, IRN, ECG -- see `SCOPES` in `extract/config/fitbit_tokens.py`).

**Onboard/verify every Fitbit device at once:**

```bash
PYTHONPATH=src python -m extract.scripts.verify_fitbit --all
```

Already-valid devices are verified silently, no prompts. Devices needing real onboarding are handled one at a time -- you'll be prompted before each browser sign-in, so you always know which window belongs to which device. You'll get a batch summary at the end (already verified / newly onboarded / failed), with a re-run command for any failures.

> ⚠️ **Tokens expire every 7 days** while the Google OAuth app is in Testing mode (refresh tokens for test users have a short lifespan) -- re-run `verify_fitbit` per device periodically until the app is moved to Production mode.

### Atmotube (static API key, per-site)

Atmotube auth is a **static API key per study site** (not per-device) -- stored at `extract/config/secrets/atmotube/api_key_<site>.json` with an `api_key` (or `key`) field. Devices are identified by MAC address, set as `access: !ENV ATMOTUBE_0X_MAC` in `config/study_registry.yml`.

**Onboarding a physical Atmotube device involves a few pieces, verified with `verify_atmotube.py`:**

1. Pair the physical device to the Atmotube phone app and make sure "upload historical data to cloud" is enabled -- otherwise the cloud API sees the device but no data (`latest_data` stays empty).
2. Add the device to `config/study_registry.yml` with its MAC as `access`.
3. Make sure the site's API key file exists at `extract/config/secrets/atmotube/api_key_<site>.json`.
4. Confirm the API key can see the device at all -- independent of any date range:

   ```bash
   PYTHONPATH=src python -m extract.scripts.verify_atmotube check --device atmotube_01
   ```

   This hits `/api/v1/devices` and matches by MAC. If it's not found, you'll get the full list of MACs the key *can* see, so you can spot a typo or a device that was never paired to this account.

   To see every device a site's key can see at once (handy for reconciling against the registry):

   ```bash
   PYTHONPATH=src python -m extract.scripts.verify_atmotube check --all --site kolkata
   ```

5. Confirm live data actually comes back for a real date (isolates host/auth/response-shape issues, bypassing all the chunking/pagination logic):

   ```bash
   PYTHONPATH=src python -m extract.scripts.verify_atmotube ping --device atmotube_01
   ```

   Run this again any time you rewire auth/host handling in `atmotube_client.py`, or after generating a new API key.

---

## 3. Seed the study registry

`config/study_registry.yml` is the source of truth for devices, sites, and participant-to-device assignments (rotating-device study design). Seeding syncs that YAML into the `study.*` tables.

```bash
PYTHONPATH=src python -m load.scripts.seed_study
```

What this does (`src/load/scripts/seed_study.py` → `general/study_registry.sync_all()`):
- Upserts `study.devices` and `study.participants` in place -- current metadata only, not history.
- Appends/closes-out rows in `study.registry`, which is **append-only history**. The pair `(device_id, user_start)` is what determines insert-vs-update (this is the `uq_device_period` unique constraint) -- an existing `user_start` with a changed `user_end` just closes out that period; it never rewrites `participant_site`, `device_site`, `recruit_start`, etc. on past rows.

It's safe to re-run any time you edit `config/study_registry.yml` (new participant, new device, closing out a device-user period, etc.) -- the sync is idempotent.

On success you'll see a one-line summary:
```
✅ Seeded: 12 devices, 6 participants, 12 user assignments
```

---

## 4. Backfill old CSV-based data

Before devices were paired to the live Atmo Cloud API, some Atmotube history exists only as CSVs exported from the Atmotube app (May–June, in this study's case). `src/extract/scripts/backfill_atmotube.py` converts those into the exact payload shape `atmotube_client.py`'s live `extract_raw_data()` produces, then pushes them through the normal `load_raw_data()` path -- so `transform/parse/atmotube_parser.py` only ever has to handle one input shape, and backfilled rows get the same `raw.ingests` treatment (same `pulled_at`-per-batch convention) as a live pull.

### Before running

1. **Seed the registry first** (step 3) -- the script maps each CSV's MAC address to a `device_id` via `config/study_registry.yml`'s `devices:` list. Unmapped MACs are skipped with a loud warning, not silently dropped.
2. **Drop the CSVs in place.** They're gitignored (real participant data), so you need to add them yourself:
   ```
   src/extract/config/secrets/atmotube/backfill/
   ```
   Filename convention, exactly as the Atmotube app exports it:
   ```
   <MAC>_<start date DD-Mon-YYYY>_<end date DD-Mon-YYYY>.csv
   e.g. C3CBE16AE294_01-May-2026_12-Jun-2026.csv
   ```
   One device can have multiple CSV files (multiple export windows) -- they get merged into one continuous record set per MAC, deduplicated by timestamp.

### Running it

Dry run first -- parses and reports without writing anything:

```bash
PYTHONPATH=src python -m extract.scripts.backfill_atmotube --dry-run
```

Check the output for:
- Any `⚠️ Skipping '<file>'` -- filename didn't match the `<MAC>_<...>.csv` convention.
- Any `UNKNOWN_DEVICE(mac=...)` -- MAC not found in the registry; fix `study_registry.yml` (or re-run step 3) before loading.
- Any `❌ CONFLICTING values at <timestamp>` -- the same timestamp appeared with different values across overlapping export files. These are logged loudly and kept (first occurrence wins) but flagged for manual review; true identical-duplicate overlaps (e.g. the boundary day of two adjacent exports) are deduplicated silently.

Once the dry run looks clean, write into `raw.ingests` for real:

```bash
PYTHONPATH=src python -m extract.scripts.backfill_atmotube --write-db
```

Or scope it to one device at a time:

```bash
PYTHONPATH=src python -m extract.scripts.backfill_atmotube --device atmotube_01 --write-db
```

You can also dump the parsed result to a JSON file for inspection instead of loading it (useful for a quick diff before trusting `--write-db`):

```bash
PYTHONPATH=src python -m extract.scripts.backfill_atmotube --out /tmp/atmotube_backfill_preview.json
```

### After backfilling

Backfilled rows land in `raw.ingests` with `ingest_method = 'csv_manual'` and a `"source": "csv_backfill"` marker inside the payload (useful for later auditing, distinguishing them from live pulls without relying on timestamp clustering). They still need to go through the normal transform step to land in `atmotube.readings` -- run the pipeline's transform/load step (`python -m load.load` or the relevant slice of `src/main.py`'s daily job) same as you would for a live pull.

Fields the CSV export doesn't include (`pm05_num`, `pm1_num`, `pm10_num`, `pm25_num`, `pm_size`, satellite/SNR fields) are legitimately `null` on backfilled rows -- not a bug.

---

## 5. Set up the auto-fetch pipeline (scheduled daily run)

`src/main.py` is the long-running entry point: it registers **one daily job** with APScheduler that wraps the full `extract → load_raw → transform → load_processed` sequence, then blocks forever. Per-device failures are already isolated deeper in the pipeline (logged to `raw.pipeline`); this layer only handles run-level retry and alerting a human if the *whole* run fails.

**Do steps 1–3 first** (DB up, devices onboarded, registry seeded) -- the scheduled job is the same sequence as a manual run, just on a timer.

### Configure the run time

Set these in `deploy/.env` (falls back to `22:45 UTC` if unset):

```
PIPELINE_RUN_HOUR=22
PIPELINE_RUN_MINUTE=45
```

**Times are UTC**, not local server time -- check what "tonight" maps to:

```bash
date -u
```

Other tunables in the same file, all optional:

```
PIPELINE_RETRY_ATTEMPTS=3         # run-level retries (e.g. DB briefly unreachable) -- NOT per-device retries
PIPELINE_RETRY_WAIT_SECONDS=60    # fixed wait between retries
SLACK_BOT_TOKEN=xoxb-...          # optional -- omit to skip Slack, failures still land in the logs
SLACK_ALERT_CHANNEL=#pipeline-notifications
```

If you set `SLACK_BOT_TOKEN`, the bot must also be **invited to the channel** (`/invite @YourBotName` in Slack) or alerts will fail with `not_in_channel`.

### Option A -- just leave it running tonight (quickest)

For a one-off overnight run, you don't need systemd -- just keep the process alive in a terminal that won't close when you disconnect (e.g. `tmux`/`screen`), since `main.py` blocks forever once started:

```bash
tmux new -s multidevice_pipeline
PYTHONPATH=src python src/main.py
# Ctrl+B then D to detach -- the process keeps running
```

You'll see:
```
Starting scheduler -- daily run at 22:45 UTC
```

It stays idle until the scheduled time, then runs the full sequence once. Reattach any time with `tmux attach -t multidevice_pipeline` to watch it, or check back in the morning. If the process happens to be down right at the scheduled minute (laptop sleep, etc.), `misfire_grace_time=3600` means it'll still fire if you're back within an hour of the scheduled time -- otherwise that day's run is skipped and it waits for tomorrow.

To stop it: reattach and `Ctrl+C`, or `tmux kill-session -t multidevice_pipeline`.

### Option B -- supervised by systemd (for it to survive reboots / run unattended long-term)

A reference unit file is checked in at `deploy/systemd/multidevice_schedule.service`. Copy it to the real systemd path and edit the paths/user for your machine:

```bash
sudo cp deploy/systemd/multidevice_schedule.service /etc/systemd/system/
sudo nano /etc/systemd/system/multidevice_schedule.service
```

Update at minimum:
- `ExecStart=` -- path to your conda env's `python` + `src/main.py`
- `WorkingDirectory=` -- repo root
- `EnvironmentFile=` -- path to `deploy/.env`
- `User=` -- the user to run as

Then enable and start it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now multidevice_schedule.service
```

`Restart=on-failure` / `RestartSec=10` means systemd restarts the process itself if it crashes -- this is separate from the pipeline's own run-level retry inside `main.py`.

Useful commands:

```bash
sudo systemctl status multidevice_schedule.service     # is it running?
journalctl -u multidevice_schedule.service -f          # tail logs live
sudo systemctl restart multidevice_schedule.service    # after editing deploy/.env (e.g. changed run time)
```

### After it runs

- Check `raw.pipeline` for per-device success/failed/partial rows, and `raw.ingests` for the actual payloads.
- A full run-level failure (all retries exhausted) posts to Slack if configured, and always logs at `ERROR` either way -- search the journal/terminal output for `[NOTIFY]`.
- Nothing needs to be re-triggered manually after a failed night -- tomorrow's scheduled run tries again, and `extract.py`'s incremental start-date logic (via `raw.pipeline`'s last successful fetch) means it picks up from where it left off rather than re-pulling everything.

---

## Quick reference

| Task | Command |
|---|---|
| Bring up DB + Grafana | `cd deploy && docker compose up -d` |
| Disposable test DB | `./deploy/postgres/test_db.sh <command>` |
| Onboard/verify one Fitbit | `PYTHONPATH=src python -m extract.scripts.verify_fitbit <device_id>` |
| Onboard/verify all Fitbits | `PYTHONPATH=src python -m extract.scripts.verify_fitbit --all` |
| Check an Atmotube is registered | `PYTHONPATH=src python -m extract.scripts.verify_atmotube check --device <device_id>` |
| Live-pull check for an Atmotube | `PYTHONPATH=src python -m extract.scripts.verify_atmotube ping --device <device_id>` |
| Seed study registry | `PYTHONPATH=src python -m load.scripts.seed_study` |
| Backfill CSVs (dry run) | `PYTHONPATH=src python -m extract.scripts.backfill_atmotube --dry-run` |
| Backfill CSVs (write to DB) | `PYTHONPATH=src python -m extract.scripts.backfill_atmotube --write-db` |
| Check current UTC time (for scheduling) | `date -u` |
| Run the scheduler now (foreground, tmux) | `PYTHONPATH=src python src/main.py` |
| Check systemd service status | `sudo systemctl status multidevice_schedule.service` |
| Tail systemd logs live | `journalctl -u multidevice_schedule.service -f` |