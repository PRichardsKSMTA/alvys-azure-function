import logging
import os
import time
import uuid
from typing import Optional, Tuple
from urllib.parse import urlparse, parse_qs

import requests


def _init_env() -> None:
    try:
        from dotenv import load_dotenv  # optional; safe if not installed
        load_dotenv(override=False)
    except Exception:
        pass


def _sanitize_env_value(value: str) -> str:
    return value.strip().strip("'").strip('"')


def _validate_flow_url(url: str) -> None:
    p = urlparse(url)
    if p.scheme != "https":
        raise RuntimeError("ERROR_FLOW_URL must be https")
    if "/triggers/" not in p.path:
        raise RuntimeError("ERROR_FLOW_URL path must contain /triggers/")
    if not ("/paths/invoke" in p.path or "/run" in p.path):
        raise RuntimeError("ERROR_FLOW_URL path must include /paths/invoke or /run")
    q = parse_qs(p.query)
    for key in ("api-version", "sp", "sv", "sig"):
        if key not in q or not q[key]:
            raise RuntimeError(f"ERROR_FLOW_URL missing required query parameter: {key}")


def _get_flow_url() -> str:
    _init_env()
    raw = os.environ.get("ERROR_FLOW_URL", "")
    if not raw:
        raise RuntimeError(
            "ERROR_FLOW_URL is not set. Set it to the full HTTP POST trigger URL "
            "including sp, sv, and sig query parameters."
        )
    url = _sanitize_env_value(raw)
    _validate_flow_url(url)
    return url


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def send_error_notification(
    function_name: str,
    message: str,
    stack_trace: str,
    correlation_id: Optional[str] = None,
) -> Tuple[int, str]:
    url = _get_flow_url()
    payload = {
        "status": "error",
        "functionName": function_name,
        "message": message,
        "timestamp": _utc_now_iso(),
        "details": {
            "stackTrace": stack_trace,
            "correlationId": correlation_id or str(uuid.uuid4()),
        },
    }
    resp = requests.post(url, json=payload, timeout=20)
    status = resp.status_code
    text = getattr(resp, "text", "")
    if status >= 400:
        logging.error("Flow POST failed: status=%s body=%s", status, text[:500])
    resp.raise_for_status()
    return status, text
