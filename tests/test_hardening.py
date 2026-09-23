from datetime import timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from notification_service.api.schemas import CampaignInput, parse_event
from notification_service.channels import ChannelRegistry
from notification_service.db.models import Delivery, Outbox, Request, now
from notification_service.main import create_app
from notification_service.outbox import OutboxPublisher
from notification_service.preferences import PreferencesService, PreferenceInput
from notification_service.requests import RequestService
from notification_service.retry import PermanentError, TemporaryError
from pydantic import ValidationError
from sqlalchemy import select

from test_workers import Auth, SMTP, envelope, setup_delivery


def test_only_supported_channel_and_bounded_urgent_campaigns(event):
    assert parse_event(event).channel == 'email'
    with pytest.raises(ValidationError):
        parse_event({**event, 'channel': 'sms'})
    campaign = dict(request_id=uuid4(), template_id='announcement',
                    audience={'kind': 'all'}, data={'headline': 'Hi', 'message': 'Hi'})
    with pytest.raises(ValidationError):
        CampaignInput(**campaign, priority='urgent')


async def test_own_preferences_require_user_auth(sessions, settings):
    user_id = uuid4()

    def authenticate(request):
        assert request.url.path == '/auth/me'
        return httpx.Response(200, json={'id': str(user_id)}) if request.headers[
                                                                     'Authorization'] == 'Bearer user-access' else httpx.Response(
            401)

    async with httpx.AsyncClient(transport=httpx.MockTransport(authenticate)) as auth_http:
        app = create_app(settings, sessions, user_http=auth_http)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://api') as client:
            assert (await client.get('/api/v1/preferences/me')).status_code == 401
            client.headers['Authorization'] = 'Bearer user-access'
            assert (await client.get('/api/v1/preferences/me')).json()['enabled'] is True
            response = await client.put('/api/v1/preferences/me', json={'enabled': False})
            assert response.status_code == 200
            assert (await client.get('/api/v1/preferences/me')).json()['enabled'] is False
            assert (await client.put('/api/v1/preferences/me', json={'user_id': str(uuid4())})).status_code == 422
            client.headers['Authorization'] = 'Bearer ' + 'o' * 32
            assert (await client.get('/api/v1/preferences/me')).status_code == 401


@pytest.mark.parametrize('body',
                         [{'enabled': False}, {'channels': {'email': False}}, {'categories': {'account': False}}])
async def test_opt_out_after_enqueue_suppresses_email(sessions, event, body):
    smtp = SMTP()
    processor, message = await setup_delivery(sessions, event, Auth(), smtp)
    await PreferencesService(sessions).set(UUID(event['payload']['user_id']), PreferenceInput(**body))
    await processor.handle(message)
    assert smtp.sent == []
    async with sessions() as session:
        row = await session.get(Delivery, message.entity_id)
        assert row.status == 'skipped'
        assert row.last_error_code == 'preferences_disabled'


async def test_channels_use_adapter_and_reject_unregistered():
    smtp = SMTP()
    registry = ChannelRegistry({'email': smtp})
    await registry.send('email', 'recipient', 'rendered', 'message-id')
    assert smtp.sent == [('recipient', 'rendered', 'message-id')]
    with pytest.raises(PermanentError):
        await registry.send('push', 'recipient', 'rendered', 'message-id')


async def test_transient_outage_recovers_after_eight_attempts(sessions, event):
    smtp = SMTP()
    smtp.error = TemporaryError('smtp_network')
    worker, message = await setup_delivery(sessions, event, Auth(), smtp)
    for _ in range(8):
        await worker.handle(message)
        async with sessions() as session:
            row = await session.get(Delivery, message.entity_id)
            assert row.status == 'retry_wait'
            row.next_attempt_at = now() - timedelta(seconds=1)
            await session.commit()
        message = await envelope(sessions, 'send')
    smtp.error = None
    await worker.handle(message)
    assert len(smtp.sent) == 1


async def test_urgent_and_bulk_have_separate_outbox_publishers(sessions, event):
    await RequestService(sessions).accept('operator', 'campaigns', CampaignInput(
        request_id=uuid4(), template_id='announcement', audience={'kind': 'all'},
        data={'headline': 'bulk', 'message': 'bulk'}))
    urgent = await RequestService(sessions).accept('auth', 'events', parse_event(event))

    class Broker:
        def __init__(self):
            self.sent = []

        async def publish(self, message, route):
            self.sent.append((message, route))

    broker = Broker()
    publisher = OutboxPublisher(sessions, broker, lane='urgent')
    assert await publisher.tick() == 1
    assert broker.sent[0][1] == 'urgent.prepare'
    assert str(broker.sent[0][0].entity_id) == urgent['id']
    assert await publisher.tick() == 0


async def test_stable_event_id_survives_service_restart_and_default_channel(sessions, event):
    first = await RequestService(sessions).accept('auth', 'events', parse_event(event))
    again = await RequestService(sessions).accept('auth', 'events', parse_event({**event, 'channel': 'email'}))
    assert again['id'] == first['id']
    async with sessions() as session:
        assert len((await session.scalars(select(Request))).all()) == 1


async def test_preference_lookup_cannot_send_after_lost_lease(sessions, event, monkeypatch):
    smtp = SMTP()
    processor, message = await setup_delivery(sessions, event, Auth(), smtp)

    async def delayed_lookup(self, *args):
        async with sessions() as session:
            row = await session.get(Delivery, message.entity_id)
            row.lease_token = uuid4()
            await session.commit()
        return True

    monkeypatch.setattr(PreferencesService, 'allows', delayed_lookup)
    await processor.handle(message)
    assert not smtp.sent


async def test_wire_envelope_keeps_legacy_fields(sessions, event):
    await RequestService(sessions).accept('auth', 'events', parse_event(event))
    async with sessions() as session:
        row = await session.scalar(select(Outbox))
        assert set(row.payload) == {'schema_version', 'message_id', 'kind', 'entity_id', 'generation', 'error_code'}
