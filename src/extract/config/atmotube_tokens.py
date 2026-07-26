# src/extract/config/atmotube_tokens.py
"""
Atmotube Cloud static API key 
    -   Each device has its own MAC addresses in config/study_registry.yml (as !ENV variables) (NO GENERATED TOKENS)
    -   API key is shared across all devices per study site  client (client_id/client_secret) is shared across all devices
NOTE: When a new study site with a new API key is dispatched update sites in #config/study_registry.yml
"""

import json
from pathlib import Path

# ============================================================================================================


CONFIG_DIR = Path(__file__).resolve().parent
SECRETS_DIR = CONFIG_DIR / "secrets" / "atmotube"

# ============================================================================================================

def get_api_key(site: str, key_path: str | None = None) -> str:
    """
    Returns the Atmotube API key for a given site.
    key_path, if provided, overrides the default filename convention
    (SECRETS_DIR / f"api_key_{site}.json") — pass study_registry.yml's sites[site]['atmotube_api_key_path'] here once that's wired up.
    """
    path = Path(key_path) if key_path else SECRETS_DIR / f"api_key_{site}.json"
    if not path.exists():
        raise FileNotFoundError(f"Atmotube API key not found for site '{site}' at {path}")
    raw = json.loads(path.read_text())
    key = raw.get("api_key") or raw.get("key")
    if not key:
        raise KeyError(f"No 'api_key' or 'key' field found in {path}")
    return key