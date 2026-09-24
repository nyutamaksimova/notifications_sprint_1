from datetime import UTC, timedelta
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select, update, func

from notification_service.api.schemas import StrictModel
from notification_service.db.models import Request, Delivery, RetryCommand, now
from notification_service.outbox import enqueue


class RetryInput(StrictModel):
    retry_id: UUID


class Operations:
    def __init__(self, sessions):
        self.sessions = sessions

    async def retry(self, kind, entity_id, retry_id):
        model = Request if kind == 'prepare' else Delivery
        key = (kind, entity_id, retry_id)
        async with self.sessions() as session:
            # Lock the target before checking the journal: concurrent replays serialize.
            row = await session.scalar(select(model).where(model.id == entity_id).with_for_update())
            if row is None:
                raise HTTPException(404, 'Task not found')
            old = await session.get(RetryCommand, key)
            if old:
                return {'id': str(entity_id), 'generation': old.generation, 'status': 'accepted'}
            if row.status != 'failed':
                raise HTTPException(409, 'Only failed tasks can be retried')
            generation = row.generation + 1
            result = await session.execute(update(model).where(model.id == entity_id,
                model.status == 'failed', model.generation == row.generation).values(
                    status='pending', generation=generation, attempts=0, next_attempt_at=now(),
                    lease_token=None, lease_until=None, last_error_code=None))
            if result.rowcount != 1:
                raise HTTPException(409, 'Task changed; repeat the command')
            session.add(RetryCommand(kind=kind, entity_id=entity_id, retry_id=retry_id, generation=generation))
            enqueue(session, kind, entity_id, generation, lane=row.lane)
            await session.commit()
            return {'id': str(entity_id), 'generation': generation, 'status': 'accepted'}

    async def failures(self, limit, offset):
        result = {}
        async with self.sessions() as session:
            for name, model in (('requests', Request), ('deliveries', Delivery)):
                rows = (await session.scalars(select(model).where(model.status.in_(['failed', 'expired']))
                        .order_by(model.id).limit(limit).offset(offset))).all()
                result[name] = [{'id': str(row.id), 'status': row.status, 'error_code': row.last_error_code,
                                 'attempts': row.attempts, 'generation': row.generation} for row in rows]
        return result

    async def backlog(self):
        active = ('accepted', 'pending', 'processing', 'retry_wait')
        timestamp = now()
        cutoff = timestamp - timedelta(seconds=180)
        result = {'target_seconds': 180}
        async with self.sessions() as session:
            for lane in ('urgent', 'bulk'):
                request_filter = (Request.lane == lane, Request.status.in_(active))
                delivery_filter = (Delivery.lane == lane, Delivery.status.in_(active))
                requests, oldest_request = (await session.execute(select(func.count(), func.min(Request.accepted_at))
                                                .where(*request_filter))).one()
                deliveries, oldest_delivery = (await session.execute(select(func.count(), func.min(Request.accepted_at))
                    .select_from(Delivery).join(Request).where(*delivery_filter))).one()
                oldest = min((date.replace(tzinfo=UTC) for date in (oldest_request, oldest_delivery) if date), default=timestamp)
                result[lane] = {'requests': requests, 'deliveries': deliveries,
                    'oldest_age_seconds': max(0, int((timestamp - oldest).total_seconds())),
                    'overdue_requests': await session.scalar(select(func.count()).select_from(Request)
                        .where(*request_filter, Request.accepted_at < cutoff)),
                    'overdue_deliveries': await session.scalar(select(func.count()).select_from(Delivery)
                        .join(Request).where(*delivery_filter, Request.accepted_at < cutoff))}
            result['failed_requests'] = await session.scalar(select(func.count()).select_from(Request).where(Request.status.in_(['failed', 'expired'])))
            result['failed_deliveries'] = await session.scalar(select(func.count()).select_from(Delivery).where(Delivery.status.in_(['failed', 'expired'])))
        return result
