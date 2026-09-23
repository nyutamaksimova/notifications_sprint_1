import os
from uuid import uuid4

import pytest
from notification_service.api.schemas import BrokerEnvelope
from notification_service.broker import Broker

pytestmark = pytest.mark.integration


async def test_unroutable_publish_fails_and_valid_message_survives():
    url = os.getenv("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("TEST_RABBITMQ_URL required; no real broker available")

    broker = Broker(url, exchange_name="notification-test-" + uuid4().hex)
    await broker.connect()
    try:
        message = BrokerEnvelope(message_id=uuid4(), kind="send", entity_id=uuid4(), generation=0)
        with pytest.raises(Exception):
            await broker.publish(message, "unbound")
        await broker.publish(message, "send")
        incoming = await broker.queues["send"].get(timeout=5)
        assert BrokerEnvelope.model_validate_json(incoming.body).entity_id == message.entity_id
        await incoming.ack()
    finally:
        for queue in broker.queues.values():
            await queue.delete(if_unused=False, if_empty=False)
        await broker.exchange.delete()
        await broker.close()
