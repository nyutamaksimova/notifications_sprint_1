import aio_pika


class Broker:
    def __init__(self, url, concurrency=5, exchange_name="notifications"):
        self.url, self.concurrency, self.exchange_name = url, concurrency, exchange_name
        self.connection = None
        self.queues = {}

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url, timeout=10)
        self.channel = await self.connection.channel(publisher_confirms=True, on_return_raises=True)
        await self.channel.set_qos(prefetch_count=self.concurrency)
        self.exchange = await self.channel.declare_exchange(self.exchange_name, aio_pika.ExchangeType.DIRECT, durable=True)
        for kind in ("prepare", "send", "failed", "urgent.prepare", "urgent.send"):
            name = f"notification.{kind}" if self.exchange_name == "notifications" else f"{self.exchange_name}.{kind}"
            queue = await self.channel.declare_queue(name, durable=True)
            await queue.bind(self.exchange, routing_key=kind)
            self.queues[kind] = queue

    async def publish(self, envelope, routing_key):
        confirmation = await self.exchange.publish(aio_pika.Message(
            body=envelope.model_dump_json(exclude={"lane"}).encode(), content_type="application/json",
            message_id=str(envelope.message_id), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=routing_key, mandatory=True, timeout=10)
        if confirmation is None or confirmation is False:
            raise ConnectionError("Publish was not confirmed")

    async def close(self):
        if self.connection:
            await self.connection.close()
