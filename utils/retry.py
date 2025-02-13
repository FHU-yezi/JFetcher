from typing import Any

from httpx import TransportError
from jkit.exceptions import RatelimitError


def get_network_request_retry_params() -> dict[str, Any]:
    return {
        "retries": 3,
        "base_delay": 5,
        "exceptions": (RatelimitError, TransportError),
    }
