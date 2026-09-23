from typing import Protocol

from notification_service.retry import PermanentError


class ChannelSender(Protocol):
    async def send(self, recipient, rendered, message_id): ...


class ChannelRegistry:
    def __init__(self, senders: dict[str, ChannelSender]):
        self.senders = dict(senders)

    async def send(self, channel, recipient, rendered, message_id):
        sender = self.senders.get(channel)
        if sender is None:
            raise PermanentError("unsupported_channel")
        await sender.send(recipient, rendered, message_id)
