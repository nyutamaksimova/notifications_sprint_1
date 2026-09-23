import asyncio
import os
from uuid import UUID
from uuid import uuid4

import pytest
from notification_service.api.schemas import parse_event, BrokerEnvelope
from notification_service.db.models import Base, Outbox, Delivery
from notification_service.operations import Operations
from notification_service.preferences import PreferencesService, PreferenceInput
from notification_service.processing import Processor
from notification_service.requests import RequestService
from notification_service.retry import PermanentError
from notification_service.templates import seed_templates
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from test_workers import Auth, SMTP
from test_workers import setup_delivery, envelope

pytestmark = pytest.mark.integration


async def test_postgres_concurrent_requests_and_workers(event):
    url = os.getenv("TEST_POSTGRES_URL")
    if not url:
        pytest.skip("TEST_POSTGRES_URL required; PostgreSQL unavailable")

    assert make_url(url).database.endswith("_test"), "Only dedicated test databases are allowed"

    schema = "test_" + uuid4().hex
    admin = create_async_engine(url)
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with sessions() as session:
            await seed_templates(session)
        a, b = await asyncio.gather(*[RequestService(sessions).accept("auth", "events", parse_event(event))
                                      for _ in range(2)])
        assert a["id"] == b["id"]
        smtp = SMTP()
        worker = Processor(sessions, Auth(), smtp)
        async with sessions() as session:
            row = await session.scalar(select(Outbox).where(Outbox.routing_key == "urgent.prepare"))
            prepare = BrokerEnvelope.model_validate(row.payload)
        await asyncio.gather(worker.handle(prepare), worker.handle(prepare))
        async with sessions() as session:
            row = await session.scalar(select(Outbox).where(Outbox.routing_key == "urgent.send"))
            send = BrokerEnvelope.model_validate(row.payload)
        await asyncio.gather(worker.handle(send), worker.handle(send))
        assert len(smtp.sent) == 1
        async with sessions() as session:
            assert (await session.get(Delivery, send.entity_id)).status == "sent"

        user_id = UUID(event['payload']['user_id'])
        await asyncio.gather(
            *[PreferencesService(sessions).set(user_id, PreferenceInput(enabled=False)) for _ in range(2)])
        assert (await PreferencesService(sessions).get(user_id)).enabled is False
        await PreferencesService(sessions).set(user_id, PreferenceInput())
        event['event_id'] = str(uuid4())

        smtp.error = PermanentError('smtp_recipient_rejected')
        worker, send = await setup_delivery(sessions, event, Auth(), smtp)
        await worker.handle(send)
        retry_id = uuid4()
        results = await asyncio.gather(
            *[Operations(sessions).retry('send', send.entity_id, retry_id) for _ in range(2)])
        assert results[0] == results[1]
        smtp.error = None
        await worker.handle(await envelope(sessions, 'send'))
        assert len(smtp.sent) == 2
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()
