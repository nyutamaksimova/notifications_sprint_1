from uuid import uuid4
from datetime import timedelta
from sqlalchemy import select, update, or_
from time import monotonic
from notification_service.logging import event
from notification_service.api.schemas import BrokerEnvelope
from notification_service.db.models import Outbox, now


def enqueue(session, kind, entity_id, generation, available_at=None, error_code=None, lane="bulk"):
    envelope = BrokerEnvelope(message_id=uuid4(), kind=kind, entity_id=entity_id,
                              generation=generation, error_code=error_code, lane=lane)
    routing_key = f"urgent.{kind}" if lane == "urgent" and kind != "failed" else kind
    session.add(Outbox(routing_key=routing_key, payload=envelope.model_dump(mode="json", exclude={"lane"}),
                       dedup_key=f"{kind}:{entity_id}:{generation}",
                       available_at=available_at or now()))


class OutboxPublisher:
    def __init__(self, sessions, broker, lane=None):
        self.sessions, self.broker = sessions, broker
        self.routes = ["urgent.prepare", "urgent.send"] if lane == "urgent" else (["prepare", "send", "failed"] if lane == "bulk" else None)

    async def tick(self):
        started = monotonic()
        token = uuid4()
        async with self.sessions() as session:
            row = await session.scalar(select(Outbox).where(
                Outbox.routing_key.in_(self.routes) if self.routes is not None else True,
                Outbox.published_at.is_(None), Outbox.available_at <= now(),
                or_(Outbox.lease_until.is_(None), Outbox.lease_until < now()))
                .order_by(Outbox.available_at).with_for_update(skip_locked=True).limit(1))
            if row is None:
                return 0
            row.lease_token, row.lease_until = token, now() + timedelta(seconds=60)
            row.attempts += 1
            await session.commit()
        try:
            await self.broker.publish(BrokerEnvelope.model_validate(row.payload), row.routing_key)
        except Exception:
            values = dict(lease_token=None, lease_until=None,
                          available_at=now() + timedelta(seconds=min(300, 2 ** min(row.attempts, 8))))
            result = 0
        else:
            values = dict(published_at=now(), lease_token=None, lease_until=None)
            result = 1
        async with self.sessions() as session:
            await session.execute(update(Outbox).where(Outbox.id == row.id, Outbox.lease_token == token).values(**values))
            await session.commit()
        event('published' if result else 'publish_failed', message_id=row.payload['message_id'],
              entity_id=row.payload['entity_id'], attempt=row.attempts,
              error_code=None if result else 'broker_publish_failed',
              duration_ms=round((monotonic() - started) * 1000))
        return result
