from uuid import uuid4

import httpx
import pytest
from notification_service.clients.auth import AuthClient
from notification_service.clients.auth import Recipient
from notification_service.clients.smtp import build_message
from notification_service.rendering import RenderedEmail
from notification_service.retry import PermanentError, TemporaryError
from notification_service.retry import RecipientMissing


async def test_auth_recipient_and_missing_user():
    user = uuid4()

    def handler(request):
        assert request.headers["Authorization"] == "Bearer reader"
        if request.url.path.endswith(str(user)):
            return httpx.Response(200, json={"id": str(user), "email": "a@example.org",
                                             "first_name": "Анна", "last_name": None})
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = AuthClient("http://auth", "reader", http)
        assert (await client.get_recipient(user)).first_name == "Анна"
        with pytest.raises(RecipientMissing):
            await client.get_recipient(uuid4())


@pytest.mark.parametrize("status,permanent", [(401, True), (403, True), (429, False), (503, False)])
async def test_auth_classifies_errors(status, permanent):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(status))) as http:
        with pytest.raises(PermanentError if permanent else TemporaryError):
            await AuthClient("http://auth", "reader", http).get_recipient(uuid4())


def test_smtp_message_has_one_recipient_and_two_parts(settings):
    message = build_message(settings, Recipient(id=uuid4(), email="a@example.org"),
                            RenderedEmail("Привет", "<p>Привет</p>", "Привет"), "<delivery@cinema>")
    assert message["To"] == "a@example.org"
    assert message["Message-ID"] == "<delivery@cinema>"
    assert [part.get_content_type() for part in message.iter_parts()] == ["text/plain", "text/html"]
