# ./src/extract/extract.py
"""
API pull logic: one pull per registry.usr row (usr_id), threaded across usrs. NOTE: Must be invoked by APScheduler.

THIS IS THE LAST MODULE TO KNOW ABT CREDENTIALS (cred). 
    resolve_usr() --> a usr row into {"usr_id", "type", "acc_id", "cred"}, which goes to the client and nowhere else. 
    Everything returned from here (and everything downstream: transform, load, raw.*, readings.*) is keyed on usr_id + type.

Dates: the usr's window [started_at, ended_at) in registry.usr is the source of truth; the yml only supplies credentials.
Every pull is clipped to that window, so two usrs that shared a device (same Atmotube MAC, different participants) never pull each other's days.
"Today" is the UTC date (everything in the pipeline is UTC).

Each returned entry carries the window that was actually pulled, "window": (start, end_inclusive), 
    so the caller can record what was scanned in raw.pull_coverage (see main.py / load/coverage.py). 
    transform and load ignore this key.

PULL CAP (max_days): one pull covers at most the LAST max_days days of what it would otherwise pull. 
    A brand-new usr or a long outage would otherwise be a months-long historical load held in memory (and, for Fitbit, longer than an access token lives). 
    The part that is cut off is not lost: it was never recorded as scanned (raw.pull_coverage), so the backfill step finds it as a gap and pulls it in bounded jobs.

FAILED PULLS: a usr whose pull failed (or came back incomplete) has no entry in the result, by design. 
    So that the caller can still tell "nothing to pull" from "pull failed", 
    every failure of the LAST extract_all_usrs() call is also listed in LAST_FORWARDFILL_FAILURES (one short, URL-free string per usr). 
    main.py reports them in the Slack summary.

CONSOLE OUTPUT: usrs are pulled IN PARALLEL (MAX_PARALLEL_USRS at once), so lines from different usrs interleave. Every line printed
    here carries "usr <id> [type]": a "▶ ... pulling" line when that usr's thread starts and a "[n/total] ... pulled / FAILED" line when it finishes. 
    Progress lines printed INSIDE a client carry whatever the client puts in them. 
    To read one usr at a time, run with max_workers=1:   python -c "from extract.extract import extract_all_usrs as f; f(max_workers=1)"
"""

import time
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from general.registry import load_registry
from general.log_pipeline import last_successful_fetch
from general.utils import resolve_usr

from general.connect_db import connect_db

from extract.clients import fitbit6_client
from extract.clients import atmotube2_client
# ============================================================================================================


CLIENT_REGISTRY = {  # keyed by registry.usr.type
    "fitbit6": fitbit6_client.extract_raw_data,
    "atmotube2": atmotube2_client.extract_raw_data,
}

# Outer cap on usrs pulled at the same time. Each client fans out further internally
#   -   fitbit6_client:   MAX_WORKERS_PER_USR requests at once (paced per token by its own rate limiter)
#   -   atmotube2_client: MAX_WORKERS_PER_USR chunk requests at once, but EVERY Atmotube request (all usrs) first takes a slot from the
#                         client's shared rate limiter (50/min of the 60/min site-wide limit), so extra usr threads just queue.
# Total in-flight requests are therefore bounded by the limiters, not by this number. Lower it only to quiet the console.
MAX_PARALLEL_USRS = 12

# Must match raw.ingests.ingest_kind's CHECK constraint (deploy/postgres/init/03_raw.sql).
# 'csv_backfill' is never produced here (go to src/extract/scripts/add_csv_atmotube2.py.)
VALID_INGEST_METHODS = {"api_forwardfill", "api_backfill", "csv_backfill"}

# A scheduled run should only ever cover a day or two.
# Anything longer (a brand-new usr falling back to its window start, or a long outage) is a historical load: every page of every data type, held in memory.
LONG_PULL_WARN_DAYS = 14

# What went wrong in the LAST extract_all_usrs() call: ["usr 5 [atmotube2]: RuntimeError: incomplete pull, failed: [...]", ...].
# Emptied at the start of every call and only ever mutated in place, so `from extract.extract import LAST_FORWARDFILL_FAILURES` stays valid.
# Texts never contain a URL (an Atmotube URL contains the MAC).
LAST_FORWARDFILL_FAILURES: list[str] = []

_print_lock = Lock()

# ============================================================================================================


def _say(msg: str) -> None:
    """Thread-safe print (flushed: under systemd stdout is a pipe and would otherwise arrive late)."""
    with _print_lock:
        print(msg, flush=True)


def _describe_error(e: BaseException) -> str:
    """Never str(e) for HTTP errors: requests embeds the URL, and an Atmotube URL contains the MAC."""
    if isinstance(e, (RuntimeError, ValueError, LookupError)):
        return f"{type(e).__name__}: {str(e)[:300]}"
    status = getattr(getattr(e, "response", None), "status_code", None)
    return type(e).__name__ + (f" (HTTP {status})" if status else "")


def load_usr_rows(conn) -> list[dict]:
    """
    One row per registry.usr entry, with all its windows: {"usr_id", "type", "acc_id", "windows": [(start, end), ...]}.
    Windows are half-open [start, end), oldest first; end NULL = open. A usr with no assignment is not returned.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT u.id, u.type, u.acc_id, a.started_at, a.ended_at
            FROM registry.usr u
            JOIN registry.usr_assignment a ON a.usr_id = u.id
            ORDER BY u.id, a.started_at
        """)
        rows: dict[int, dict] = {}
        for usr_id, type_, acc_id, s, e in cur.fetchall():
            rows.setdefault(usr_id, {"usr_id": usr_id, "type": type_, "acc_id": acc_id, "windows": []})["windows"].append((s, e))
        return list(rows.values())


def _resolve_pull(config: dict, usr: dict, last_success, end: date) -> tuple[dict, date, date] | str:
    """
    One usr -> (client_usr, start, end_inclusive), or a string saying why there is nothing to pull.
    Both dates are INCLUSIVE civil dates (what the clients take); the usr windows are half-open.
    Only the LATEST window that has started by `end` is pulled. Earlier windows (and the gap between windows,
    which may belong to someone else) are never pulled here; backfill heals what is left of earlier windows.
    A backfill can reuse this with last_success=None to get that window whole.
    """
    started = [w for w in usr["windows"] if w[0] <= end]
    if not started:
        return f"first window starts {usr['windows'][0][0]}, after pull end {end}"
    u_start, u_end = started[-1]
    if u_start > end:
        return f"window starts {u_start}, after pull end {end}"

    try:
        client_usr = resolve_usr(config, usr)
    except ValueError as e:
        return f"cannot resolve credentials: {e}"

    # Re-pull from the day of the last success (upserts make the overlap harmless), else from the start of the window.
    first_start = last_success.astimezone(timezone.utc).date() if last_success is not None else u_start
    start = max(first_start, u_start)

    end_incl = end
    if u_end is not None:
        end_incl = min(end_incl, u_end - timedelta(days=1))   # half-open window

    if start > end_incl:
        return "already pulled through the end of its window"

    return client_usr, start, end_incl


def _failed_parts(type_: str, payload) -> list[str]:
    """What is missing from a payload that came back without raising. Empty list = complete."""
    if not isinstance(payload, dict):
        return []
    if type_ == "fitbit6":
        # None = that data type FAILED (an empty list is a successful, empty pull). A failed profile is tolerated.
        return [k for k, v in payload.items() if v is None and k != "profile"]
    if type_ == "atmotube2":
        chunks = payload.get("chunks") or []
        if not chunks:
            return ["no chunks"]
        return [f"{c['start']}..{c['end']}" for c in chunks if c.get("status") != "success"]
    return []


def _pull_one_usr(client_usr: dict, start_date: str, end_date: str, started: dict | None = None):
    """
    Returns (usr_id, payload_or_None, error_or_None). An incomplete pull comes back as (usr_id, None, error), never as partial data.
    `started`, if given, receives {usr_id: time.monotonic()} when this usr's thread begins (the caller uses it for timings).
    """
    usr_id, type_ = client_usr["usr_id"], client_usr["type"]
    if started is not None:
        started[usr_id] = time.monotonic()
    _say(f"   ▶ usr {usr_id} [{type_}] pulling {start_date} → {end_date}")

    client_fn = CLIENT_REGISTRY.get(type_)
    if client_fn is None:
        return usr_id, None, NotImplementedError(f"No client registered for type={type_}")
    try:
        payload = client_fn(client_usr, start_date, end_date)
    except Exception as e:
        return usr_id, None, e

    failed = _failed_parts(type_, payload)
    if failed:
        return usr_id, None, RuntimeError(f"incomplete pull, failed: {failed}")
    return usr_id, payload, None


def extract_all_usrs(config_path="config/study_registry.yml", end_date=None,
                    max_workers=MAX_PARALLEL_USRS, ingest_kind="api_forwardfill",
                    max_days: int | None = None) -> dict[str, dict[int, dict]]:
    """
    Returns { type: { usr_id: {"payload", "ingest_kind", "window"} } }, where window = (start, end_inclusive) as dates.
    A usr that failed or had nothing to pull has no entry; failures are also listed in LAST_FORWARDFILL_FAILURES.
    max_days: pull at most the last N days per usr (see PULL CAP in the module docstring); None = no cap.
    """
    if max_days is not None and max_days < 1:
        raise ValueError(f"max_days must be >= 1, got {max_days}")
    if ingest_kind not in VALID_INGEST_METHODS:
        raise ValueError(f"Invalid ingest_kind={ingest_kind!r}; must be one of {VALID_INGEST_METHODS}")

    LAST_FORWARDFILL_FAILURES.clear()

    config = load_registry(config_path)
    end_date_obj = date.fromisoformat(end_date) if end_date else datetime.now(timezone.utc).date()

    conn = connect_db()
    try:
        usr_rows = load_usr_rows(conn)
        last_success = {u["usr_id"]: last_successful_fetch(conn, u["usr_id"]) for u in usr_rows}
    finally:
        conn.close()

    if not usr_rows:
        print("❌ No rows in registry.usr")
        return {}

    plans = []
    for u in usr_rows:
        plan = _resolve_pull(config, u, last_success[u["usr_id"]], end_date_obj)
        if isinstance(plan, str):
            _say(f"   ⏭️ Skipping usr {u['usr_id']} [{u['type']}]: {plan}")
            if plan.startswith("cannot resolve credentials"):      # a config problem, not "nothing to do": surface it
                LAST_FORWARDFILL_FAILURES.append(f"usr {u['usr_id']} [{u['type']}]: {plan[:200]}")
            continue
        client_usr, start, end_incl = plan
        if max_days is not None and (end_incl - start).days + 1 > max_days:
            capped_start = end_incl - timedelta(days=max_days - 1)
            _say(f"   ✂️ usr {u['usr_id']} [{u['type']}]: pull capped to the last {max_days} days ({capped_start} → {end_incl}); "
                 f"{start} → {capped_start - timedelta(days=1)} is left to the backfill step")
            start = capped_start
        days = (end_incl - start).days + 1
        if days > LONG_PULL_WARN_DAYS:
            _say(f"   ⚠️ usr {u['usr_id']} [{u['type']}]: pulling {days} days ({start} → {end_incl}) in one go -- historical load")
        plans.append((client_usr, start, end_incl))

    all_data: dict[str, dict[int, dict]] = {}
    windows = {client_usr["usr_id"]: (start, end_incl) for client_usr, start, end_incl in plans}

    print(f"--- Pulling {len(plans)} of {len(usr_rows)} usr row(s) (end={end_date_obj}) ---")
    if not plans:
        return {}

    started: dict[int, float] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(plans)))) as executor:
        future_to_usr = {
            executor.submit(_pull_one_usr, client_usr, str(start), str(end_incl), started): client_usr
            for client_usr, start, end_incl in plans
        }
        for future in as_completed(future_to_usr):
            client_usr = future_to_usr[future]
            type_, usr_id = client_usr["type"], client_usr["usr_id"]
            _, payload, error = future.result()

            done += 1
            took = time.monotonic() - started.get(usr_id, time.monotonic())
            tag = f"[{done}/{len(plans)}] usr {usr_id} [{type_}]"

            if payload is None:
                reason = _describe_error(error or RuntimeError("no payload"))
                _say(f"   ❌ {tag} FAILED after {took:.0f}s: {reason}")
                LAST_FORWARDFILL_FAILURES.append(f"usr {usr_id} [{type_}]: {reason}")
                continue

            all_data.setdefault(type_, {})[usr_id] = {
                "payload": payload,
                "ingest_kind": ingest_kind,
                "window": windows[usr_id],
            }
            _say(f"   ✅ {tag} pulled in {took:.0f}s")

    for type_, per_usr in all_data.items():
        print(f"  ✅ {type_}: {len(per_usr)} usr_id(s) pulled")
    for type_ in {u["type"] for u, _, _ in plans} - all_data.keys():
        print(f"  ⚠️ {type_}: No successful pulls")

    return all_data