from uuid import uuid4
from datetime import timedelta

import httpx
from sqlalchemy import select, func

from notification_service.db.models import Delivery, Outbox, Request, now
from notification_service.main import create_app
from notification_service.retry import PermanentError
from test_workers import Auth, SMTP, setup_delivery, envelope


async def test_operator_retry_is_idempotent_and_never_resends_success(sessions, settings, event):
    smtp = SMTP()
    smtp.error = PermanentError('smtp_recipient_rejected')
    processor, message = await setup_delivery(sessions, event, Auth(), smtp)
    await processor.handle(message)
    url = f'/api/v1/deliveries/{message.entity_id}/retry'
    body = {'retry_id': str(uuid4())}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(settings, sessions)), base_url='http://api') as client:
        assert (await client.post(url, json=body)).status_code == 401
        client.headers['Authorization'] = 'Bearer ' + 'a' * 32
        assert (await client.post(url, json=body)).status_code == 403
        client.headers['Authorization'] = 'Bearer ' + 'o' * 32
        failures = await client.get('/api/v1/operations/failures')
        assert failures.status_code == 200
        assert failures.json()['deliveries'][0]['id'] == str(message.entity_id)
        first = await client.post(url, json=body)
        assert first.status_code == 202
        assert (await client.post(url, json=body)).json() == first.json()
        smtp.error = None
        await processor.handle(await envelope(sessions, 'send'))
        assert len(smtp.sent) == 1
        assert (await client.post(url, json=body)).json() == first.json()
        assert (await client.post(url, json={'retry_id': str(uuid4())})).status_code == 409
    async with sessions() as session:
        assert (await session.get(Delivery, message.entity_id)).status == 'sent'
        assert await session.scalar(select(func.count()).select_from(Outbox).where(Outbox.routing_key == 'urgent.send')) == 2


async def test_backlog_reports_overdue_urgent_work(sessions, settings, event):
    processor, message = await setup_delivery(sessions, event, Auth(), SMTP())
    async with sessions() as session:
        row = await session.get(Delivery, message.entity_id)
        request = await session.get(Request, row.request_id)
        request.accepted_at = now() - timedelta(seconds=200)
        await session.commit()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(settings, sessions)), base_url='http://api',
                                headers={'Authorization': 'Bearer ' + 'o' * 32}) as client:
        response = await client.get('/api/v1/operations/backlog')
        assert response.status_code == 200
        assert response.json()['urgent']['overdue_deliveries'] == 1
        assert response.json()['urgent']['oldest_age_seconds'] >= 200
        await processor.handle(message)
        assert (await client.get('/api/v1/operations/backlog')).json()['urgent']['overdue_deliveries'] == 0
