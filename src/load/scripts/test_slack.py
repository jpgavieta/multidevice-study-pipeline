"""
Standalone test for main.py's notify_failure() / Slack delivery.
Does NOT touch the DB, extract, transform, or load -- purely exercises the
Slack notification path in isolation.

USAGE:
PYTHONPATH=src python -m load.scripts.test_slack

Confirms:
    1. deploy/.env has SLACK_BOT_TOKEN loaded correctly
    2. the bot token is valid and has chat:write scope
    3. the bot has actually been invited to SLACK_ALERT_CHANNEL
    4. the message renders as expected in Slack

If this succeeds, you should see a message land in #pipeline-notifs (or whatever SLACK_ALERT_CHANNEL is set to) within a few seconds.
"""

from datetime import datetime, timezone

# Reuses main.py's own notify_failure() + config loading (including its load_dotenv() call) 
# -- so this test exercises the EXACT same code path run_daily_pipeline() would use on a real failure, not a reimplementation.
from main import notify_failure, SLACK_BOT_TOKEN, SLACK_ALERT_CHANNEL


def main():
    print(f"SLACK_BOT_TOKEN loaded: {'yes' if SLACK_BOT_TOKEN else 'NO -- check deploy/.env'}")
    print(f"SLACK_ALERT_CHANNEL: {SLACK_ALERT_CHANNEL}")

    if not SLACK_BOT_TOKEN:
        print("❌ Aborting -- SLACK_BOT_TOKEN not set, nothing to test.")
        return

    now = datetime.now(timezone.utc).isoformat()
    print("Sending test notification...")
    notify_failure(
        subject="[TEST] MultiDevice Datakit notify_failure() check",
        message=(
            f"This is a manual test of the Slack notification path, sent at {now}. "
            f"If you see this in {SLACK_ALERT_CHANNEL}, delivery is working correctly. "
            f"No real pipeline failure occurred."
        ),
    )
    print("Done -- check the logs above for any Slack delivery warnings, "
          f"and check {SLACK_ALERT_CHANNEL} in Slack for the actual message.")


if __name__ == "__main__":
    main()