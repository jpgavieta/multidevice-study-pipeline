# src/extract/config/tokens.py
"""
Central credential resolver for all device APIs Each _tokens.py module must NEVER touch each other.


Each service (Fitbit, Atmotube, ...) has its own auth module with its own logic, since auth mechanisms differ (OAuth vs. static API key). 
This is the single import surface clients use, so _client.py scripts don't need to know which specific module their credentials come from.

TO ADD A NEW DEVICE API:
    1. Create a new module in extract/config/ named <type>_tokens.py (type as in study_registry.yml).
    2. Add a new function here that calls the new module's credential getter.
    3. Put its secrets under extract/config/secrets/<type>/.
"""

from .fitbit6_tokens import get_access_token as _fitbit6_get_access_token
from .atmotube2_tokens import get_api_key as _atmotube2_get_api_key


def get_fitbit_token(acc_id: str, allow_interactive: bool = False) -> str:
    """acc_id is the Google account id from `access:` in study_registry.yml (e.g. "0207"), not a device id."""
    return _fitbit6_get_access_token(acc_id, allow_interactive=allow_interactive)


def get_atmotube_api_key() -> str:
    """One static key shared by every Atmotube device (not per site, not per device)."""
    return _atmotube2_get_api_key()