from typing import Any

from httpx import TransportError
from jkit.exceptions import RatelimitError

NETWORK_REQUEST_RETRY_PARAMS: dict[str, Any] = {
    "retries": 3,
    "base_delay": 5,
    "exceptions": (RatelimitError, TransportError),
}
