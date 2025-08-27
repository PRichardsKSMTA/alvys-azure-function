import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

import uuid
import requests
from utils.alerts import send_error_notification, _ENDPOINT


def test_send_error_notification_posts_payload(monkeypatch):
    captured = {}

    def fake_post(url, json=None, timeout=None):
        captured['url'] = url
        captured['json'] = json
        captured['timeout'] = timeout
        class Response:
            status_code = 200
        return Response()

    monkeypatch.setattr(requests, 'post', fake_post)
    fake_uuid = uuid.UUID('12345678-1234-5678-1234-567812345678')
    monkeypatch.setattr(uuid, 'uuid4', lambda: fake_uuid)

    send_error_notification('func', 'message', 'trace')

    assert captured['url'] == _ENDPOINT
    payload = captured['json']
    assert payload['status'] == 'error'
    assert payload['functionName'] == 'func'
    assert payload['message'] == 'message'
    assert payload['details']['stackTrace'] == 'trace'
    assert payload['details']['correlationId'] == str(fake_uuid)
    assert captured['timeout'] == 10
