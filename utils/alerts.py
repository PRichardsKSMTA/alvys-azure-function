#!/usr/bin/env python
"""Error notification utilities for Azure Functions."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Optional

import requests

_ENDPOINT = (
    "https://prod-168.westus.logic.azure.com:443/workflows/c1616e65b35d40238ae046c60d5b372a/triggers/manual/paths/invoke?api-version=2016-06-01"
)


def send_error_notification(
    function_name: str,
    message: str,
    stack_trace: str,
    correlation_id: Optional[str] = None,
) -> None:
    """POST a standardized error payload to the Logic App endpoint."""
    payload = {
        "status": "error",
        "functionName": function_name,
        "message": message,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "details": {
            "stackTrace": stack_trace,
            "correlationId": correlation_id or str(uuid.uuid4()),
        },
    }
    try:
        requests.post(_ENDPOINT, json=payload, timeout=10)
    except Exception as exc:  # pragma: no cover - best effort only
        logging.error("Failed to send error notification: %s", exc)
