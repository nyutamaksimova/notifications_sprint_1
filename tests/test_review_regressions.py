import asyncio
import json
import logging
import os
import shutil
import subprocess
from uuid import uuid4

import pytest
from notification_service.api.schemas import BrokerEnvelope, parse_event
from notification_service.outbox import OutboxPublisher
from notification_service.processing import Processor
from notification_service.requests import RequestService
from notification_service.workers.runtime import consume_message
from notification_service.workers.runtime import wait_for_in_flight

from test_workers import Auth, SMTP


async def test_missing_entity_publishes_diagnostic_before_ack(sessions):
    envelope = BrokerEnvelope(message_id=uuid4(), entity_id=uuid4(), generation=0, kind='send')

    class Message:
        body = envelope.model_dump_json().encode()
        processed = False

        async def ack(self):
            self.processed = True

    message = Message()
    diagnostics = []

    class Broker:
        async def publish(self, diagnostic, key):
            assert not message.processed
            diagnostics.append((diagnostic, key))

    await consume_message(message, 'send', Processor(sessions, Auth(), SMTP()), Broker(), asyncio.Event())
    assert message.processed
    assert diagnostics[0][0].entity_id == envelope.entity_id
    assert diagnostics[0][0].error_code == 'entity_missing'
    assert diagnostics[0][1] == 'failed'


async def test_diagnostic_failure_does_not_ack_original(sessions):
    envelope = BrokerEnvelope(message_id=uuid4(), entity_id=uuid4(), generation=0, kind='send')

    class Message:
        body = envelope.model_dump_json().encode()
        processed = False
        acked = False
        requeued = False

        async def ack(self):
            self.acked = True

        async def nack(self, requeue):
            self.requeued = requeue

    class Broker:
        async def publish(self, diagnostic, key):
            raise ConnectionError()

    stop = asyncio.Event()
    stop.set()
    message = Message()
    await consume_message(message, 'send', Processor(sessions, Auth(), SMTP()), Broker(), stop)
    assert not message.acked
    assert message.requeued


async def test_shutdown_drains_in_flight_callbacks_before_cancelling():
    finished = asyncio.Event()

    async def send():
        await asyncio.sleep(0.01)
        finished.set()

    task = asyncio.create_task(send())
    await wait_for_in_flight({task}, timeout=1)
    assert finished.is_set()
    assert not task.cancelled()


async def test_shutdown_cancels_callback_after_grace_period():
    async def send_forever():
        await asyncio.Event().wait()

    task = asyncio.create_task(send_forever())
    await wait_for_in_flight({task}, timeout=0.01)
    assert task.cancelled()


async def test_failed_publish_logs_sanitized_diagnostic(sessions, event, caplog):
    class Broker:
        async def publish(self, envelope, key):
            raise ValueError('private-address@example.org credential-secret')

    await RequestService(sessions).accept('auth', 'events', parse_event(event))
    with caplog.at_level(logging.INFO, logger='notification'):
        await OutboxPublisher(sessions, Broker()).tick()
    logs = [json.loads(record.message) for record in caplog.records if record.name == 'notification']
    assert logs and logs[0]['event'] == 'publish_failed'
    assert logs[0]['attempt'] == 1
    assert logs[0]['error_code'] == 'broker_publish_failed'
    assert 'credential-secret' not in caplog.text
    assert 'private-address' not in caplog.text


@pytest.mark.parametrize('filename', ['docker-compose.yml', 'docker-compose.test.yml'])
def test_compose_api_does_not_wait_for_broker(filename):
    if not shutil.which('docker'):
        pytest.skip('Docker CLI unavailable')
    result = subprocess.run(['docker', 'compose', '--env-file', '.env.example', '-f', filename,
                             'config', '--format', 'json'], capture_output=True, text=True, encoding='utf-8',
                            env={**os.environ, 'NOTIFICATION_ENV_FILE': '.env.example'})
    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)
    assert 'rabbitmq' not in config['services']['api']['depends_on']
