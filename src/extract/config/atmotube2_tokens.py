# src/extract/config/atmotube_tokens.py
"""
Atmotube Cloud static API key 
    -   Each device has its own MAC addresses in config/study_registry.yml (as !ENV variables) (NO GENERATED TOKENS)
    -   API key is shared across all devices (REGARDLESS of study site) 
"""

import json
from pathlib import Path

# ============================================================================================================

TYPE_DIR = "atmotube2"                         # device type in study_registry.yml == folder under secrets/
CONFIG_DIR = Path(__file__).resolve().parent   # .../src/extract/config
API_KEY_PATH = CONFIG_DIR / "secrets" / TYPE_DIR / "api_key.json"

def get_api_key(key_path: Path | str | None = None) -> str:
    path = Path(key_path) if key_path else API_KEY_PATH
    if not path.exists():
        raise FileNotFoundError(f"Atmotube API key not found at {path}")
    raw = json.loads(path.read_text())
    key = raw.get("api_key") or raw.get("key")
    if not key:
        raise KeyError(f"No 'api_key' or 'key' field found in {path}")
    return key