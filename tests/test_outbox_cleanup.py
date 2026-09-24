from datetime import timedelta

from sqlalchemy import select

from notification_service.db.models import Outbox, now
from notification_service.outbox import OutboxPublisher


async def test_cleanup_removes_only_old_published_rows(sessions):
    async with sessions() as session:
        old = Outbox(routing_key='send', payload={'message_id': 'old', 'entity_id': 'old'},
                     dedup_key='old', available_at=now(), published_at=now() - timedelta(days=10))
        recent = Outbox(routing_key='send', payload={'message_id': 'recent', 'entity_id': 'recent'},
                        dedup_key='recent', available_at=now(), published_at=now())
        pending = Outbox(routing_key='send', payload={'message_id': 'pending', 'entity_id': 'pending'},
                         dedup_key='pending', available_at=now())
        session.add_all([old, recent, pending])
        await session.commit()
    deleted = await OutboxPublisher(sessions, object()).cleanup(7 * 86400)
    assert deleted == 1
    async with sessions() as session:
        rows = (await session.scalars(select(Outbox))).all()
        assert {row.dedup_key for row in rows} == {'recent', 'pending'}
