import asyncio
from contextlib import suppress
from datetime import timedelta
from uuid import UUID, uuid4
from time import monotonic

from sqlalchemy import select, update

from notification_service.db.models import Request, Delivery, Template, now
from notification_service.outbox import enqueue
from notification_service.rendering import render_email
from notification_service.retry import TemporaryError, PermanentError, RecipientMissing, retry_delay
from notification_service.logging import event
from notification_service.channels import ChannelRegistry
from notification_service.preferences import PreferencesService


class LostLease(Exception):
    pass


class MissingEntity(Exception):
    pass


class Processor:
    def __init__(self, sessions, auth, smtp):
        self.sessions, self.auth, self.smtp = sessions, auth, smtp
        self.channels = ChannelRegistry({"email": smtp})

    async def handle(self, envelope):
        started = monotonic()
        model = Request if envelope.kind == "prepare" else Delivery
        token = uuid4()
        async with self.sessions() as session:
            row = (await session.execute(update(model).where(
                model.id == envelope.entity_id, model.generation == envelope.generation,
                model.status.in_(["pending", "accepted", "retry_wait"]),
                model.next_attempt_at <= now()
            ).values(status="processing", lease_token=token, lease_until=now() + timedelta(seconds=120),
                     attempts=model.attempts + 1).returning(model))).scalar_one_or_none()
            await session.commit()
            if row is None:
                if await session.get(model, envelope.entity_id) is None:
                    raise MissingEntity()
                return
        heartbeat = asyncio.create_task(self.heartbeat(model, row.id, token))
        try:
            if envelope.kind == "prepare":
                await self.prepare(row, token)
            else:
                await self.send(row, token)
        except LostLease:
            return
        except Exception as error:
            await self.fail(model, row.id, token, envelope.kind, error)
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
            event('handling_finished', entity_id=row.id, message_id=envelope.message_id,
                  kind=envelope.kind, attempt=row.attempts, duration_ms=round((monotonic() - started) * 1000))

    async def heartbeat(self, model, entity_id, token):
        while True:
            await asyncio.sleep(30)
            try:
                async with self.sessions() as session:
                    await session.execute(update(model).where(model.id == entity_id, model.lease_token == token,
                        model.status == "processing").values(lease_until=now() + timedelta(seconds=120)))
                    await session.commit()
            except Exception:
                return

    async def owned(self, session, model, entity_id, token):
        row = await session.scalar(select(model).where(
            model.id == entity_id, model.lease_token == token, model.status == "processing",
            model.lease_until > now()).with_for_update())
        if row is None:
            raise LostLease()
        return row

    async def prepare(self, request, token):
        if request.audience["kind"] == "all":
            page = await self.auth.list_user_ids(100, request.cursor, request.audience_cutoff)
            ids, cursor = page.items, page.next_cursor
            if cursor and (cursor == request.cursor or not ids):
                raise PermanentError("auth_invalid_cursor")
        else:
            ids, cursor = [UUID(value) for value in request.audience["user_ids"]], None
        async with self.sessions() as session:
            row = await self.owned(session, Request, request.id, token)
            for user_id in set(ids):
                existing = await session.scalar(select(Delivery.id).where(
                    Delivery.request_id == row.id, Delivery.user_id == user_id))
                if existing is None:
                    delivery = Delivery(id=uuid4(), request_id=row.id, user_id=user_id, channel=row.channel, lane=row.lane)
                    session.add(delivery)
                    enqueue(session, "send", delivery.id, 0, lane=row.lane)
            row.cursor, row.lease_token, row.lease_until = cursor, None, None
            if cursor:
                row.generation += 1
                row.status = "pending"
                row.attempts = 0  # Retry budget per page, not per whole broadcast.
                row.next_attempt_at = now()
                enqueue(session, "prepare", row.id, row.generation, lane=row.lane)
            else:
                row.status = "prepared"
            await session.commit()

            event('preparation_transition', request_id=row.id, status=row.status,
                  generation=row.generation, attempt=row.attempts)

    async def send(self, delivery, token):
        try:
            recipient = await self.auth.get_recipient(delivery.user_id)
        except RecipientMissing:
            recipient = None
        if recipient is None or recipient.email is None:
            await self.finish(delivery.id, token, "skipped")
            return
        async with self.sessions() as session:
            request = await session.get(Request, delivery.request_id)
            template = await session.get(Template, request.template_version_id)
            try:
                rendered = render_email(template, recipient, request.data)
            except Exception as error:
                raise PermanentError("template_invalid") from error
        if not await PreferencesService(self.sessions).allows(delivery.user_id, delivery.channel, request.category):
            await self.finish(delivery.id, token, "skipped", "preferences_disabled")
            return
        async with self.sessions() as session:
            await self.owned(session, Delivery, delivery.id, token)
        await self.channels.send(delivery.channel, recipient, rendered, f"<{delivery.id}@notifications.cinema>")
        await self.finish(delivery.id, token, "sent")

    async def finish(self, entity_id, token, status, error_code=None):
        async with self.sessions() as session:
            row = await self.owned(session, Delivery, entity_id, token)
            row.status, row.lease_token, row.lease_until = status, None, None
            row.last_error_code = error_code
            if status == "sent":
                row.sent_at = now()
            await session.commit()
            event('delivery_transition', delivery_id=row.id, request_id=row.request_id,
                  status=status, attempt=row.attempts)

    async def fail(self, model, entity_id, token, kind, error):
        async with self.sessions() as session:
            try:
                row = await self.owned(session, model, entity_id, token)
            except LostLease:
                return
            delay = None if isinstance(error, PermanentError) else retry_delay(row.attempts)
            if isinstance(error, TemporaryError):
                delay = min(60 if row.lane == "urgent" else 300, 10 * 2 ** min(row.attempts - 1, 5))
            code = getattr(error, "code", "processing_error")
            if delay is not None and isinstance(error, TemporaryError) and error.delay is not None:
                delay = max(delay, min(1800, max(10, error.delay)))
            row.generation += 1
            row.last_error_code, row.lease_token, row.lease_until = code, None, None
            if delay is None:
                row.status = "failed"
                enqueue(session, "failed", row.id, row.generation, error_code=code)
            else:
                row.status = "retry_wait"
                row.next_attempt_at = now() + timedelta(seconds=delay)
                enqueue(session, kind, row.id, row.generation, row.next_attempt_at, lane=row.lane)
            await session.commit()
            event('processing_failure', entity_id=row.id, kind=kind, status=row.status,
                  attempt=row.attempts, error_code=code)


class Recovery:
    def __init__(self, sessions):
        self.sessions = sessions

    async def tick(self):
        count = 0
        for model, kind in ((Request, "prepare"), (Delivery, "send")):
            async with self.sessions() as session:
                rows = (await session.scalars(select(model).where(model.status == "processing",
                        model.lease_until < now() - timedelta(seconds=30))
                        .with_for_update(skip_locked=True).limit(100))).all()
                for row in rows:
                    row.generation += 1
                    row.lease_until, row.lease_token = None, None
                    row.last_error_code = "lease_expired"
                    row.next_attempt_at = now()
                    row.status = "retry_wait"
                    enqueue(session, kind, row.id, row.generation, lane=row.lane)
                    count += 1
                await session.commit()
                for row in rows:
                    event('lease_recovered', entity_id=row.id, kind=kind, status=row.status,
                          attempt=row.attempts, error_code='lease_expired')
        return count
