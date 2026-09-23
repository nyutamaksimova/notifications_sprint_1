import asyncio
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from notification_service.api.schemas import parse_event, BrokerEnvelope, CampaignInput
from notification_service.clients.auth import Recipient, UserPage
from notification_service.db.models import Request, Delivery, Outbox, now
from notification_service.db.models import Template
from notification_service.outbox import OutboxPublisher
from notification_service.processing import Processor
from notification_service.processing import Recovery
from notification_service.requests import RequestService
from notification_service.retry import PermanentError
from sqlalchemy import select


class Auth:
    def __init__(self, ids=None, email="a@example.org"):
        self.ids, self.email = ids or [], email

    async def get_recipient(self, user_id):
        return Recipient(id=user_id, email=self.email, first_name="Анна")

    async def list_user_ids(self, limit, cursor, created_before):
        start = int(cursor or 0)
        end = start + limit
        return UserPage(items=self.ids[start:end], next_cursor=str(end) if end < len(self.ids) else None)


class SMTP:
    def __init__(self):
        self.sent = []
        self.error = None

    async def send(self, recipient, rendered, message_id):
        if self.error:
            raise self.error
        self.sent.append((recipient, rendered, message_id))


async def envelope(sessions, kind):
    async with sessions() as session:
        row = await session.scalar(
            select(Outbox).where(Outbox.routing_key.in_([kind, f"urgent.{kind}"])).order_by(Outbox.available_at.desc()))
        return BrokerEnvelope.model_validate(row.payload)


async def setup_delivery(sessions, event, auth, smtp):
    processor = Processor(sessions, auth, smtp)
    await RequestService(sessions).accept("auth", "events", parse_event(event))
    await processor.handle(await envelope(sessions, "prepare"))
    return processor, await envelope(sessions, "send")


async def test_preparation_and_send_are_idempotent(sessions, event):
    smtp = SMTP()
    processor, message = await setup_delivery(sessions, event, Auth(), smtp)
    await processor.handle(message)
    await processor.handle(message)
    assert len(smtp.sent) == 1
    assert "Анна" in smtp.sent[0][1].text
    async with sessions() as session:
        assert (await session.get(Delivery, message.entity_id)).status == "sent"


async def test_missing_email_is_skipped(sessions, event):
    smtp = SMTP()
    processor, message = await setup_delivery(sessions, event, Auth(email=None), smtp)
    await processor.handle(message)
    assert not smtp.sent
    async with sessions() as session:
        assert (await session.get(Delivery, message.entity_id)).status == "skipped"


async def test_mass_pages_resume_without_duplicate(sessions):
    ids = [uuid4() for _ in range(103)]
    body = CampaignInput(request_id=uuid4(), template_id="announcement", audience={"kind": "all"},
                         data={"headline": "Hi", "message": "Hello"})
    receipt = await RequestService(sessions).accept("operator", "campaigns", body)
    worker = Processor(sessions, Auth(ids), SMTP())
    first = await envelope(sessions, "prepare")
    await worker.handle(first)
    await worker.handle(first)
    await worker.handle(await envelope(sessions, "prepare"))
    async with sessions() as session:
        rows = (await session.scalars(select(Delivery))).all()
        assert len(rows) == len({row.user_id for row in rows}) == 103
        assert (await session.get(Request, UUID(receipt["id"]))).status == "prepared"


async def test_five_attempts_then_dead_letter(sessions, event):
    smtp = SMTP()
    smtp.error = RuntimeError("unexpected processing failure")
    worker, message = await setup_delivery(sessions, event, Auth(), smtp)
    for attempt in range(5):
        await worker.handle(message)
        async with sessions() as session:
            row = await session.get(Delivery, message.entity_id)
            assert row.attempts == attempt + 1
            if attempt < 4:
                assert row.status == "retry_wait"
                row.next_attempt_at = now() - timedelta(seconds=1)
                await session.commit()
        message = await envelope(sessions, "send")
    async with sessions() as session:
        assert (await session.get(Delivery, message.entity_id)).status == "failed"
    assert (await envelope(sessions, "failed")).error_code == "processing_error"


async def test_crashed_lease_is_recovered_and_old_message_ignored(sessions, event):
    smtp = SMTP()
    worker, message = await setup_delivery(sessions, event, Auth(), smtp)
    async with sessions() as session:
        row = await session.get(Delivery, message.entity_id)
        row.status, row.attempts, row.lease_token = "processing", 1, uuid4()
        row.lease_until = now() - timedelta(seconds=40)
        await session.commit()
    assert await Recovery(sessions).tick() == 1
    await worker.handle(message)
    assert not smtp.sent
    await worker.handle(await envelope(sessions, "send"))
    assert len(smtp.sent) == 1


async def test_outbox_failure_retains_record(sessions, event):
    class BrokenBroker:
        async def publish(self, envelope, routing_key):
            raise ConnectionError("unroutable")

    await RequestService(sessions).accept("auth", "events", parse_event(event))
    assert await OutboxPublisher(sessions, BrokenBroker()).tick() == 0
    async with sessions() as session:
        row = await session.scalar(select(Outbox))
        assert row.published_at is None
        assert row.attempts == 1


async def test_active_delivery_does_not_send_concurrently(sessions, event):
    entered, release = asyncio.Event(), asyncio.Event()

    class WaitingSMTP(SMTP):
        async def send(self, recipient, rendered, message_id):
            entered.set()
            await release.wait()
            await super().send(recipient, rendered, message_id)

    smtp = WaitingSMTP()
    worker, message = await setup_delivery(sessions, event, Auth(), smtp)
    first = asyncio.create_task(worker.handle(message))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await worker.handle(message)
    finally:
        release.set()
        await first
    assert len(smtp.sent) == 1


async def test_permanent_error_does_not_retry(sessions, event):
    smtp = SMTP()
    smtp.error = PermanentError('smtp_rejected')
    worker, message = await setup_delivery(sessions, event, Auth(), smtp)
    await worker.handle(message)
    async with sessions() as session:
        row = await session.get(Delivery, message.entity_id)
        assert row.status == 'failed'
        assert row.attempts == 1
    assert (await envelope(sessions, 'failed')).error_code == 'smtp_rejected'


async def test_smtp_accepted_then_crash_can_duplicate_with_same_message_id(sessions, event):
    smtp = SMTP()
    worker, message = await setup_delivery(sessions, event, Auth(), smtp)

    # Simulate death after SMTP accepted and before committing sent.
    async def crash(*args):
        raise KeyboardInterrupt('simulated process death')

    worker.finish = crash
    with pytest.raises(KeyboardInterrupt):
        await worker.handle(message)
    assert len(smtp.sent) == 1
    async with sessions() as session:
        row = await session.get(Delivery, message.entity_id)
        row.lease_until = now() - timedelta(seconds=40)
        await session.commit()
    await Recovery(sessions).tick()

    await Processor(sessions, Auth(), smtp).handle(await envelope(sessions, 'send'))
    assert len(smtp.sent) == 2
    assert smtp.sent[0][2] == smtp.sent[1][2]


async def test_movie_event_selects_template_and_all_users(sessions):
    ids = [uuid4(), uuid4()]
    body = parse_event({'event_id': str(uuid4()), 'event_type': 'movie.published',
                        'occurred_at': '2026-09-22T00:00:00Z', 'payload': {'movie_id': str(uuid4()),
                                                                           'title': 'Премьера',
                                                                           'url': 'https://cinema.example.org/new'}})
    receipt = await RequestService(sessions).accept('catalog', 'events', body)
    smtp = SMTP()
    worker = Processor(sessions, Auth(ids), smtp)
    await worker.handle(await envelope(sessions, 'prepare'))
    async with sessions() as session:
        request = await session.get(Request, UUID(receipt['id']))
        assert (await session.get(Template, request.template_version_id)).template_id == 'new_movie'
        tasks = (await session.scalars(select(Outbox).where(Outbox.routing_key == 'send'))).all()
    for task in tasks:
        await worker.handle(BrokerEnvelope.model_validate(task.payload))
    assert len(smtp.sent) == 2
    assert all('Премьера' in item[1].subject for item in smtp.sent)


async def test_repeated_auth_page_fails_instead_of_looping(sessions):
    class StuckAuth(Auth):
        async def list_user_ids(self, limit, cursor, created_before):
            return UserPage(items=[uuid4()], next_cursor='stuck')

    body = CampaignInput(request_id=uuid4(), template_id='announcement', audience={'kind': 'all'},
                         data={'headline': 'Hi', 'message': 'Hi'})
    receipt = await RequestService(sessions).accept('operator', 'campaigns', body)
    worker = Processor(sessions, StuckAuth(), SMTP())
    await worker.handle(await envelope(sessions, 'prepare'))
    await worker.handle(await envelope(sessions, 'prepare'))
    async with sessions() as session:
        row = await session.get(Request, UUID(receipt['id']))
        assert row.status == 'failed'
        assert row.last_error_code == 'auth_invalid_cursor'
