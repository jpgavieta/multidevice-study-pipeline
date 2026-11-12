# src/general/settings.py
"""
Loads and validates the pipeline settings.

    VALUES  config/pipeline_settings.yml   tunables (tracked in git, every value written out and commented)
            deploy/.env                    secrets only: SLACK_BOT_TOKEN (required), DB_*
    RULES   INT_SETTINGS below             the allowed range of each number, the only thing the code knows about them

There are NO defaults in code: a missing or out-of-range value stops startup with every problem listed at once.
An env var named in INT_SETTINGS, if set, overrides the yml (handy for one-off test runs) and is marked in the startup log.

HOW TO ADD A SETTING:  add the line to pipeline_settings.yml, add one row to INT_SETTINGS, use cfg.<attr> in the code.


    Test run:  (no DB, no start extract/load, validate pipeline_settings.yml + deploy/.env)
PYTHONPATH=src python -m general.settings --check     

"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Mapping

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
SETTINGS_PATH = REPO_ROOT / "config" / "pipeline_settings.yml"
ENV_PATH = REPO_ROOT / "deploy" / ".env"

# (yml section, yml key, cfg attribute, lo, hi, env var that overrides it)
INT_SETTINGS = [
    ("schedule",    "run_hour",           "run_hour",                   0, 23,        "PIPELINE_RUN_HOUR"),
    ("schedule",    "run_minute",         "run_minute",                 0, 59,        "PIPELINE_RUN_MINUTE"),
    ("schedule",    "catchup_hours",      "catchup_hours",              1, 24 * 14,   "PIPELINE_CATCHUP_HOURS"),
    ("schedule",    "retry_attempts",     "retry_attempts",             1, 10,        "PIPELINE_RETRY_ATTEMPTS"),
    ("schedule",    "retry_wait_seconds", "retry_wait_seconds",         0, 3600,      "PIPELINE_RETRY_WAIT_SECONDS"),
    ("forwardfill", "max_forwardfill_days", "max_forwardfill_days",      1, 366,       "PIPELINE_MAX_FORWARDFILL_DAYS"),
    ("backfill",    "max_jobs",           "backfill_max_jobs",          0, 10_000,    "PIPELINE_BACKFILL_MAX_JOBS"),
    ("backfill",    "workers",            "backfill_workers",           1, 16,        "PIPELINE_BACKFILL_WORKERS"),
    ("backfill",    "minutes",            "backfill_minutes",           0, 24 * 60,   "PIPELINE_BACKFILL_MINUTES"),
    ("backfill",    "first_run_minutes",  "backfill_first_run_minutes", 0, 7 * 24 * 60, "PIPELINE_BACKFILL_FIRST_RUN_MINUTES"),
    ("alerts",      "notify_on_ok",       "slack_notify_on_ok",         0, 1,         "SLACK_NOTIFY_ON_OK"),   # yml true/false = 1/0
]


class ConfigError(Exception):
    def __init__(self, problems: list[str]):
        super().__init__("; ".join(problems))
        self.problems = problems


def _read_yaml(path: Path = SETTINGS_PATH) -> dict:
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        raise ConfigError([f"{path} not found"])
    except yaml.YAMLError as e:
        raise ConfigError([f"{path} is not valid YAML: {e}"])
    if not isinstance(data, dict):
        raise ConfigError([f"{path} must contain sections (schedule:, backfill:, ...)"])
    return data


def _section(data: dict, name: str) -> dict:
    sec = data.get(name)
    return sec if isinstance(sec, dict) else {}


def parse_config(data: dict, environ: Mapping[str, str]) -> SimpleNamespace:
    """Validates the yml `data` + `environ`. Raises ConfigError listing EVERY problem; never echoes a secret."""
    values: dict = {}
    problems: list[str] = []
    overridden: list[str] = []

    for sec, key, attr, lo, hi, env in INT_SETTINGS:
        from_env = (environ.get(env) or "").strip()
        raw = from_env if from_env else _section(data, sec).get(key)
        where = f"{env}" if from_env else f"{sec}.{key}"

        if raw is None or raw == "":
            problems.append(f"{sec}.{key} is missing in {SETTINGS_PATH.name}")
            continue
        if isinstance(raw, float):
            problems.append(f"{where}={raw!r} is not an integer")
            continue
        try:
            val = int(raw)        # int(True) == 1, so yml `notify_on_ok: true` works
        except (TypeError, ValueError):
            problems.append(f"{where}={raw!r} is not an integer")
            continue
        if not lo <= val <= hi:
            problems.append(f"{where}={val} is out of range {lo}..{hi}")
            continue
        values[attr] = val
        if from_env:
            overridden.append(attr)

    # Slack channel: yml, or env SLACK_ALERT_CHANNEL (kept so a one-off test can point elsewhere)
    channel = (environ.get("SLACK_ALERT_CHANNEL") or "").strip() or str(_section(data, "alerts").get("slack_channel") or "").strip()
    if not channel:
        problems.append(f"alerts.slack_channel is missing in {SETTINGS_PATH.name} (quote it: an unquoted # is a YAML comment)")
    values["slack_channel"] = channel

    # Secret: deploy/.env only
    if not (environ.get("SLACK_BOT_TOKEN") or "").strip():
        problems.append("SLACK_BOT_TOKEN is required and not set in deploy/.env")
    values["slack_bot_token"] = (environ.get("SLACK_BOT_TOKEN") or "").strip()

    if problems:
        raise ConfigError(problems)
    values["env_overrides"] = overridden
    return SimpleNamespace(**values)


def load_env() -> None:
    """The ONE place deploy/.env is loaded. Never overrides a variable that is already set (e.g. by systemd's EnvironmentFile=)."""
    load_dotenv(ENV_PATH)


def load_config(environ: Mapping[str, str] = os.environ) -> SimpleNamespace:
    """For the real process: loads deploy/.env, then on any problem prints them all and exits."""
    if environ is os.environ:
        load_env()
    try:
        return parse_config(_read_yaml(), environ)
    except ConfigError as e:
        sys.exit("CONFIG ERROR:\n" + "\n".join(f"  - {p}" for p in e.problems))


def describe(cfg: SimpleNamespace) -> list[str]:
    """Log lines with the effective values. `(env)` marks a value that came from an env var instead of the yml."""
    def fmt(attr: str) -> str:
        return f"{attr}={getattr(cfg, attr)}" + ("(env)" if attr in cfg.env_overrides else "")

    return [
        f"config from {SETTINGS_PATH.name}: " + " ".join(fmt(s[2]) for s in INT_SETTINGS),
        f"slack: channel={cfg.slack_channel} token=<set>",
    ]


def slack_settings() -> tuple[str, bool]:
    """(channel, notify_on_ok) for general/notify_slack.py. Read at CALL time and never raises: alerting must not crash a run."""
    try:
        data = _read_yaml()
    except ConfigError:
        data = {}
    alerts = _section(data, "alerts")
    channel = (os.environ.get("SLACK_ALERT_CHANNEL") or "").strip() or str(alerts.get("slack_channel") or "").strip()
    flag = (os.environ.get("SLACK_NOTIFY_ON_OK") or "").strip()
    notify_on_ok = (flag != "0") if flag else bool(alerts.get("notify_on_ok", True))
    return channel, notify_on_ok


if __name__ == "__main__":
    if "--check" in sys.argv:
        load_env()
        try:
            cfg = parse_config(_read_yaml(), os.environ)
        except ConfigError as e:
            print(f"❌ {len(e.problems)} problem(s):")
            for p in e.problems:
                print(f"  - {p}")
            sys.exit(1)
        print(f"✅ {SETTINGS_PATH} + {ENV_PATH} pass validation")
        for line in describe(cfg):
            print("  " + line)
    else:
        print(__doc__)