import asyncio
from uuid import uuid4

import httpx
from notification_service.db.models import Request, Outbox
from notification_service.main import create_app
from sqlalchemy import select, func


async def test_concurrent_requests_and_conflicting_reuse(sessions, settings, event):
    app = create_app(settings, sessions)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                 headers={"Authorization": "Bearer " + "a" * 32}) as client:
        a, b = await asyncio.gather(*[client.post("/api/v1/events", json=event) for _ in range(2)])
        assert a.status_code == b.status_code == 202
        assert a.json()["id"] == b.json()["id"]
        conflict = await client.post("/api/v1/events", json={**event, "payload": {"user_id": str(uuid4())}})
        assert conflict.status_code == 409
        assert (await client.get("/api/v1/requests/" + a.json()["id"])).status_code == 200
        assert (await client.get("/api/v1/requests/" + a.json()["id"],
                                 headers={"Authorization": "Bearer " + "c" * 32})).status_code == 403
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Request)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_access_validation_and_size_limit(sessions, settings, event):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(settings, sessions)),
                                 base_url="http://test") as client:
        assert (await client.post("/api/v1/events", json=event)).status_code == 401
        client.headers["Authorization"] = "Bearer " + "a" * 32
        campaign = {"request_id": str(uuid4()), "template_id": "announcement",
                    "audience": {"kind": "all"}, "data": {"headline": "Hi", "message": "Hello"}}
        assert (await client.post("/api/v1/campaigns", json=campaign)).status_code == 403
        assert (await client.post("/api/v1/events", json={**event, "extra": 1})).status_code == 422
        assert (await client.post("/api/v1/events", content=b"x" * 262145)).status_code == 413
