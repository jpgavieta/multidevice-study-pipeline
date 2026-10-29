# src/extract/config/fitbit_tokens.py
"""
Google Health OAuth
    -   Each Fitbit ACCOUNT (a Google login, id from `access:` in study_registry.yml, e.g. "0207") has its own generated token file -- a token do NOT belong to a device.
    -   OAuth client (client_id/client_secret) is shared across all accounts
"""

import json
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs

import requests

# ============================================================================================================

# fitbit_tokens.py
TYPE_DIR = "fitbit6"                           # device type in study_registry.yml == folder under secrets/
CONFIG_DIR = Path(__file__).resolve().parent   # .../src/extract/config
API_KEY_PATH = CONFIG_DIR / "secrets" / TYPE_DIR / "client_secret.json" # OAuth client (client_id + client_secret), not a bare key
TOKENS_DIR = CONFIG_DIR / "secrets" / TYPE_DIR / "tokens"

AUTH_PORT = 8765
REDIRECT_URI = f"http://localhost:{AUTH_PORT}"
AUTH_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"

SCOPES = [
    # Steps, distance, floors, active minutes, active zone minutes, exercise sessions,
    # VO2 max, calories (total/active), altitude, sedentary periods, heart rate zones
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly",

    # Heart rate, HRV, resting heart rate, oxygen saturation (SpO2), respiratory rate,
    # body fat, weight, sleep temperature derivations
    "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly",

    # GPS data tied to a recorded exercise session, exposed in TCX format
    "https://www.googleapis.com/auth/googlehealth.location.readonly",

    # # Nutrition/food log data (participants arent using it)
    # "https://www.googleapis.com/auth/googlehealth.nutrition.readonly",

    # Sleep sessions, stages (awake/light/deep/REM), sleep summary
    "https://www.googleapis.com/auth/googlehealth.sleep.readonly",

    # Fitbit user ID + Google user ID + basic profile fields — not a time-series type
    "https://www.googleapis.com/auth/googlehealth.profile.readonly",

    # Irregular Rhythm Notification engagement status (via getIrnProfile) —
    # confirm this actually returns data for your device models before relying on it;
    # IRN is a specific feature not present on all Fitbit hardware.
    "https://www.googleapis.com/auth/googlehealth.irn.readonly",

    # ECG (electrocardiogram) readings — only relevant if your devices are
    # ECG-capable hardware (e.g. Sense/Charge with ECG sensor). Some reporting
    # suggests this may require separate approval beyond standard OAuth consent —
    # worth testing early rather than assuming it'll just work.
    "https://www.googleapis.com/auth/googlehealth.ecg.readonly",
]

# ============================================================================================================


def _load_client_secrets():
    raw = json.loads(API_KEY_PATH.read_text())
    web = raw["web"]
    return web["client_id"], web["client_secret"]


def _token_file(acc_id: str) -> Path:
    return TOKENS_DIR / f"{acc_id}.json"


class _CallbackHandler(BaseHTTPRequestHandler):
    code = None
    error = None

    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        _CallbackHandler.code = params.get("code", [None])[0]
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        if _CallbackHandler.code:
            self.wfile.write(b"<html><body>Authorized. You can close this tab.</body></html>")
        else:
            _CallbackHandler.error = params.get("error", ["unknown error"])[0]
            self.wfile.write(f"<html><body>Authorization failed: {_CallbackHandler.error}</body></html>".encode())

    def log_message(self, format, *args):
        pass


def _start_callback_server() -> tuple[HTTPServer, threading.Thread]:
    """
    Start listening for Google's redirect BEFORE the browser is opened. Binding happens here, so a busy port fails immediately (instead of after you've already signed in).
    """
    _CallbackHandler.code = None   # reset between accounts — same class, reused sequentially
    _CallbackHandler.error = None
    try:
        server = HTTPServer(("localhost", AUTH_PORT), _CallbackHandler)
    except OSError as e:
        raise RuntimeError(
            f"Cannot listen on port {AUTH_PORT} ({e}). Is another onboarding still running? "
            f"The redirect URI registered in Google Cloud is {REDIRECT_URI}."
        ) from e
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    return server, thread


def _wait_for_code(server: HTTPServer, thread: threading.Thread) -> str:
    try:
        thread.join(timeout=180)
        if _CallbackHandler.code is None:
            if _CallbackHandler.error:
                raise RuntimeError(f"Google sign-in was not approved: {_CallbackHandler.error}")
            raise TimeoutError(
                "No redirect received within 3 minutes — did you approve in "
                "the browser, or close the tab without approving?"
            )
        return _CallbackHandler.code
    finally:
        server.server_close()


def get_access_token(acc_id: str, allow_interactive: bool = True) -> str:
    token_file = _token_file(acc_id)
    if token_file.exists():
        tokens = json.loads(token_file.read_text())
        try:
            return _refresh_access_token(acc_id, tokens)
        except requests.exceptions.HTTPError:
            if not allow_interactive:
                raise
            print(f"⚠️  Saved token for account '{acc_id}' is no longer valid (refresh rejected). Re-authorizing...")
            return _run_interactive_auth(acc_id)
    if not allow_interactive:
        raise RuntimeError(
            f"No saved token for account '{acc_id}' — not onboarded yet. "
            f"Run: python -m extract.scripts.verify_fitbit {acc_id}"
        )
    return _run_interactive_auth(acc_id)


def _run_interactive_auth(acc_id: str) -> str:
    client_id, client_secret = _load_client_secrets()

    params = {
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
    }
    auth_url = f"{AUTH_ENDPOINT}?{urlencode(params)}"

    server, thread = _start_callback_server()

    print(f"Sign in to the Google account for '{acc_id}'.")
    try:
        opened = webbrowser.open(auth_url)
    except Exception:
        opened = False
    if not opened:
        print("⚠️  Python could not open a browser automatically (common on WSL/SSH).") # always printed: if no window appeared (or the wrong browser opened), paste this into any browser
    print(f"\nIf no browser window appears, open this URL yourself:\n\n{auth_url}\n")
    print("Waiting up to 3 minutes for you to approve...")

    code = _wait_for_code(server, thread)

    resp = requests.post(TOKEN_ENDPOINT, data={
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": REDIRECT_URI,
    })
    resp.raise_for_status()
    tokens = resp.json()

    token_file = _token_file(acc_id)
    token_file.parent.mkdir(parents=True, exist_ok=True)
    token_file.write_text(json.dumps(tokens, indent=2))
    print(f"✅ Authorized and saved token for account '{acc_id}'.")
    return tokens["access_token"]


def _refresh_access_token(acc_id: str, tokens: dict) -> str:
    client_id, client_secret = _load_client_secrets()
    resp = requests.post(TOKEN_ENDPOINT, data={
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": tokens["refresh_token"],
        "grant_type": "refresh_token",
    })
    if not resp.ok:
        print(f"DEBUG — Google response body: {resp.text}")
    resp.raise_for_status()
    new_tokens = resp.json()
    new_tokens.setdefault("refresh_token", tokens["refresh_token"])
    _token_file(acc_id).write_text(json.dumps(new_tokens, indent=2))
    return new_tokens["access_token"]