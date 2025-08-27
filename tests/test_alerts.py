import os
import uuid
from pathlib import Path
import importlib.util
from unittest.mock import patch
import pytest # type: ignore


def _load_alerts_module():
    module_path = Path(__file__).resolve().parents[1] / "utils" / "alerts.py"
    spec = importlib.util.spec_from_file_location("alerts", str(module_path))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_send_error_notification_posts_payload_unit():
    captured = {}

    def fake_post(url, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout

        class Response:
            status_code = 202
            text = "Accepted"
            def raise_for_status(self): return None

        return Response()

    flow_url = (
        "https://prod-168.westus.logic.azure.com:443/workflows/c1616e65b35d40238ae046c60d5b372a/"
        "triggers/manual/paths/invoke?api-version=2016-06-01&sp=/triggers/manual/run&sv=1.0&sig=abc123"
    )

    with patch.dict(os.environ, {"ERROR_FLOW_URL": flow_url}, clear=False), \
         patch("requests.post", side_effect=fake_post), \
         patch("uuid.uuid4", return_value=uuid.UUID("12345678-1234-5678-1234-567812345678")):
        alerts = _load_alerts_module()
        status, body = alerts.send_error_notification("func", "message", "trace")

    assert captured["url"] == flow_url
    payload = captured["json"]
    assert payload["status"] == "error"
    assert payload["functionName"] == "func"
    assert payload["message"] == "message"
    assert payload["details"]["stackTrace"] == "trace"
    assert payload["details"]["correlationId"] == "12345678-1234-5678-1234-567812345678"
    assert isinstance(payload["timestamp"], str)
    assert captured["timeout"] == 20
    assert 200 <= status < 300
    assert isinstance(body, str)


@pytest.mark.skipif(
    os.getenv("RUN_LIVE_FLOW_TEST") != "1",
    reason="Set RUN_LIVE_FLOW_TEST=1 to run the live Flow trigger test."
)
def test_send_error_notification_live_triggers_flow(capsys):
    flow_url = os.getenv("ERROR_FLOW_URL")
    assert flow_url, "ERROR_FLOW_URL is not set"
    assert "sig=" in flow_url, "ERROR_FLOW_URL must include the signed 'sig=' parameter"

    alerts = _load_alerts_module()
    status, body = alerts.send_error_notification(
        function_name="pytest-live",
        message="Integration probe from tests",
        stack_trace="(none)",
        correlation_id="pytest-" + uuid.uuid4().hex[:12],
    )

    print("LIVE HTTP status:", status)
    print("LIVE HTTP body first 500 chars:", body[:500])
    assert 200 <= status < 300
