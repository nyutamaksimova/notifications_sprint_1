import json
import os
from pathlib import Path
import subprocess
import time
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(os.getenv('RUN_CONTAINER_FAULT_TESTS') != '1',
                                reason='Opt-in host Docker fault tests')
ROOT = Path(__file__).resolve().parents[2]


def compose(*args):
    result = subprocess.run(['docker', 'compose', '-p', 'notification-test', '-f',
        'docker-compose.test.yml', *args], cwd=ROOT, capture_output=True, encoding='utf-8', timeout=120)
    assert result.returncode == 0, result.stderr
    return result.stdout


def request(service, path, data=None, token=None):
    source = (
        'import json, urllib.request; '
        f'body = {json.dumps(data)!r}; '
        f'headers = {({"Authorization": "Bearer " + token} if token else {})!r}; '
        'headers["Content-Type"] = "application/json"; '
        f'req = urllib.request.Request("http://localhost:8000{path}", '
        f'data=body.encode() if {data is not None!r} else None, headers=headers); '
        'print(urllib.request.urlopen(req, timeout=10).read().decode())'
    )
    return json.loads(compose('exec', '-T', service, 'python', '-c', source))


def recipient():
    return request('auth-service', '/auth/register', {
        'username': f'{uuid4().hex}@example.org', 'password': 'test-pass-123'})['id']


def campaign(user_id):
    return request('api', '/api/v1/campaigns', {
        'request_id': str(uuid4()), 'template_id': 'announcement',
        'audience': {'kind': 'users', 'user_ids': [user_id]},
        'data': {'headline': 'Recovery test', 'message': 'Recovered'}}, 'o' * 32)['id']


def wait_for_status(request_id, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = request('api', f'/api/v1/requests/{request_id}', token='o' * 32)
        if predicate(result):
            return result
        time.sleep(1)
    raise AssertionError(f'Timed out awaiting request {request_id}: {result}')


def test_broker_outage_preserves_accepted_request():
    user = recipient()
    compose('stop', 'rabbitmq')
    try:
        request_id = campaign(user)
        status = request('api', f'/api/v1/requests/{request_id}', token='o' * 32)
        assert status['preparation_status'] == 'accepted'
    finally:
        compose('start', 'rabbitmq')
    result = wait_for_status(request_id, lambda s: s['deliveries']['sent'] == 1)
    assert result['deliveries']['failed'] == 0


def test_sender_restart_preserves_retry():
    user = recipient()
    compose('stop', 'mailpit')
    try:
        request_id = campaign(user)
        wait_for_status(request_id, lambda s: s['deliveries']['retry_wait'] == 1, timeout=30)
        compose('restart', 'send')
    finally:
        compose('start', 'mailpit')
    result = wait_for_status(request_id, lambda s: s['deliveries']['sent'] == 1)
    assert result['deliveries']['failed'] == 0
