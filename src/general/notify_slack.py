# src/general/notify_slack.py
"""Test run:
PYTHONPATH=src python -m general.notify_slack
"""

import os, logging, requests

log = logging.getLogger("multidevice_study")
SLACK_API_URL = "https://slack.com/api/chat.postMessage"   # only used here

def notify(subject: str, message: str, ok: bool = False) -> None:
    """Always logs; Slack is best-effort and never raises."""
    log.log(logging.INFO if ok else logging.ERROR, f"[NOTIFY] {subject}: {message}")

    # Read env at CALL time, not import time, so it doesn't matter whether load_dotenv() ran first.
    token = os.environ.get("SLACK_BOT_TOKEN")
    channel = os.environ.get("SLACK_ALERT_CHANNEL")
    if not token or not channel:
        log.warning("Slack not configured -- logged locally only.")
        return
    if ok and os.environ.get("SLACK_NOTIFY_ON_OK", "1") == "0":
        return

    emoji = ":white_check_mark:" if ok else ":rotating_light:"
    try:
        resp = requests.post(SLACK_API_URL,
                            headers={"Authorization": f"Bearer {token}"},
                            json={"channel": channel, "text": f"{emoji} *{subject}*\n{message}"},
                            timeout=10)
        payload = resp.json()
        if not payload.get("ok"):
            log.warning(f"Slack notify failed: {payload.get('error')} (channel={channel})")
    except Exception as e:
        log.warning(f"Slack notify raised an exception: {e}")

def notify_failure(subject: str, message: str) -> None:
    notify(subject, message, ok=False)

if __name__ == "__main__":
    # Manual smoke test (no DB / extract / load): sends one failure-style and one success-style message to SLACK_ALERT_CHANNEL.
    from datetime import datetime, timezone
    from pathlib import Path
    from dotenv import load_dotenv

    # Only here, not at module level: in production main.py / systemd has already loaded the env.
    load_dotenv(Path(__file__).resolve().parents[2] / "deploy" / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    token, channel = os.environ.get("SLACK_BOT_TOKEN"), os.environ.get("SLACK_ALERT_CHANNEL")
    print(f"SLACK_BOT_TOKEN loaded: {'yes' if token else 'NO -- check deploy/.env'}")
    print(f"SLACK_ALERT_CHANNEL: {channel or 'NOT SET'}")
    if not (token and channel):
        raise SystemExit("❌ Aborting -- Slack not configured, nothing to test.")

    now = datetime.now(timezone.utc).isoformat()
    notify_failure("[TEST] failure path", f"Manual test at {now}. No real failure occurred.")
    notify("[TEST] success path", f"Manual test at {now}.", ok=True)
    if os.environ.get("SLACK_NOTIFY_ON_OK", "1") == "0":
        print("ℹ️ SLACK_NOTIFY_ON_OK=0, so the success message was suppressed on purpose.")
    print(f"Done -- check the log lines above for Slack warnings, then check {channel} for 2 messages.")