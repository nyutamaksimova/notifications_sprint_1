import asyncio
import signal
from time import monotonic
from uuid import uuid4

import httpx

from notification_service.api.schemas import BrokerEnvelope
from notification_service.broker import Broker
from notification_service.clients.auth import AuthClient
from notification_service.clients.smtp import SMTPClient
from notification_service.config import Settings
from notification_service.db.session import session_factory
from notification_service.logging import configure, event
from notification_service.outbox import OutboxPublisher
from notification_service.operations import Operations
from notification_service.processing import Processor, Recovery, MissingEntity


async def consume_message(message, kind, processor, broker, stop):
    try:
        envelope = BrokerEnvelope.model_validate_json(message.body)
        if envelope.kind != kind:
            raise ValueError('wrong queue')
    except ValueError:
        envelope = BrokerEnvelope(message_id=uuid4(), entity_id=uuid4(),
                                  kind='failed', generation=0, error_code='invalid_envelope')
    try:
        if envelope.error_code == 'invalid_envelope':
            await broker.publish(envelope, 'failed')
        else:
            try:
                await processor.handle(envelope)
            except MissingEntity:
                diagnostic = envelope.model_copy(update={'kind': 'failed', 'message_id': uuid4(),
                                                         'error_code': 'entity_missing'})
                await broker.publish(diagnostic, 'failed')
                event('entity_missing', entity_id=envelope.entity_id, message_id=envelope.message_id)
        await message.ack()
    except Exception:
        event('consumer_unavailable', entity_id=envelope.entity_id, message_id=envelope.message_id,
              kind=kind, error_code='processing_or_diagnostic_unavailable')
        await pause(stop, 2)
        if not message.processed:
            await message.nack(requeue=True)


async def run(kind):
    configure()
    settings = Settings()
    sessions = session_factory(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with httpx.AsyncClient() as http:
            processor = Processor(sessions, AuthClient(settings.auth_url, settings.auth_reader_token.get_secret_value(), http),
                                  SMTPClient(settings))
            while not stop.is_set():
                broker = Broker(settings.rabbitmq_url.get_secret_value(), settings.concurrency)
                try:
                    await broker.connect()
                    if kind == "publisher":
                        publisher, recovery = OutboxPublisher(sessions, broker, lane=settings.worker_lane), Recovery(sessions)
                        next_report = 0
                        while not stop.is_set():
                            await recovery.tick()
                            if settings.worker_lane == 'urgent' and monotonic() >= next_report:
                                backlog = await Operations(sessions).backlog()
                                urgent = backlog['urgent']
                                if urgent['overdue_requests'] or urgent['overdue_deliveries']:
                                    event('urgent_slo_exceeded', **urgent)
                                next_report = monotonic() + 60
                            published = await publisher.tick()
                            if not published:
                                await pause(stop, 1)
                    else:
                        semaphore = asyncio.Semaphore(settings.concurrency)
                        async def consume(message):
                            async with semaphore:
                                await consume_message(message, kind, processor, broker, stop)
                        route = f"urgent.{kind}" if settings.worker_lane == "urgent" else kind
                        tag = await broker.queues[route].consume(consume)
                        await stop.wait()
                        await broker.queues[route].cancel(tag)
                except Exception:
                    event("worker_connection_error", kind=kind)
                    await pause(stop, 5)
                finally:
                    await broker.close()
    finally:
        await sessions.kw["bind"].dispose()


async def pause(stop, seconds):
    try:
        await asyncio.wait_for(stop.wait(), seconds)
    except TimeoutError:
        pass
