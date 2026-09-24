from contextlib import asynccontextmanager
import secrets
import httpx
from uuid import UUID

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from starlette.responses import JSONResponse

from notification_service.api.schemas import EventInput, CampaignInput
from notification_service.config import Settings
from notification_service.db.session import session_factory
from notification_service.requests import RequestService
from notification_service.preferences import PreferencesService, PreferenceInput
from notification_service.operations import Operations, RetryInput


class BodyLimit:
    def __init__(self, app, limit=262144):
        self.app, self.limit = app, limit

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        chunks, size = [], 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            size += len(message.get("body", b""))
            if size > self.limit:
                return await JSONResponse({"detail": "Request too large"}, 413)(scope, receive, send)
            chunks.append(message)
            if not message.get("more_body", False):
                break
        async def buffered_receive():
            return chunks.pop(0) if chunks else await receive()
        await self.app(scope, buffered_receive, send)


def create_app(settings=None, sessions=None, user_http=None):
    supplied = sessions
    @asynccontextmanager
    async def lifespan(app):
        if not hasattr(app.state, "sessions"):
            app.state.settings = settings or Settings()
            app.state.sessions = session_factory(app.state.settings)
        yield
        if supplied is None:
            await app.state.sessions.kw["bind"].dispose()

    app = FastAPI(lifespan=lifespan, title="Cinema notifications")
    if settings is not None:
        app.state.settings = settings
        app.state.sessions = sessions or session_factory(settings)
    app.add_middleware(BodyLimit)
    bearer = HTTPBearer(auto_error=False)

    async def principal(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)):
        if credentials:
            for name, token in app.state.settings.service_tokens.items():
                if secrets.compare_digest(credentials.credentials.encode(), token.get_secret_value().encode()):
                    return name
        raise HTTPException(401, "Invalid service credentials")

    async def current_user(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)):
        if not credentials:
            raise HTTPException(401, "User credentials required")
        if any(secrets.compare_digest(credentials.credentials.encode(), value.get_secret_value().encode())
               for value in app.state.settings.service_tokens.values()):
            raise HTTPException(401, "User credentials required")
        async def resolve(client):
            try:
                response = await client.get(str(app.state.settings.auth_url).rstrip("/") + "/auth/me",
                    headers={"Authorization": f"Bearer {credentials.credentials}"}, timeout=5)
            except httpx.HTTPError:
                raise HTTPException(503, "Authentication unavailable") from None
            if response.status_code in (401, 403):
                raise HTTPException(401, "Invalid user credentials")
            if response.status_code != 200:
                raise HTTPException(503, "Authentication unavailable")
            try:
                return UUID(response.json()["id"])
            except (ValueError, KeyError, TypeError):
                raise HTTPException(503, "Invalid authentication response") from None
        if user_http is not None:
            return await resolve(user_http)
        async with httpx.AsyncClient() as client:
            return await resolve(client)

    @app.get("/api/v1/preferences/me")
    async def preferences(user_id: UUID = Depends(current_user)):
        return await PreferencesService(app.state.sessions).get(user_id)

    @app.put("/api/v1/preferences/me")
    async def set_preferences(body: PreferenceInput, user_id: UUID = Depends(current_user)):
        return await PreferencesService(app.state.sessions).set(user_id, body)

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request, error):
        return JSONResponse({"detail": "Database unavailable"}, 503)

    @app.get("/health/live")
    async def live():
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready():
        async with app.state.sessions() as session:
            version = await session.scalar(text("SELECT version_num FROM alembic_version"))
            if version != "0003":
                raise HTTPException(503, "Migrations required")
        return {"status": "ok"}

    @app.post("/api/v1/events", status_code=202)
    async def event(body: EventInput, service: str = Depends(principal)):
        allowed = {"auth": "user.registered", "catalog": "movie.published"}
        if allowed.get(service) != body.event_type:
            raise HTTPException(403, "Event not permitted")
        return await RequestService(app.state.sessions).accept(service, "events", body)

    @app.post("/api/v1/campaigns", status_code=202)
    async def campaign(body: CampaignInput, service: str = Depends(principal)):
        if service != "operator":
            raise HTTPException(403, "Campaigns not permitted")
        return await RequestService(app.state.sessions).accept(service, "campaigns", body)

    @app.get("/api/v1/requests/{request_id}")
    async def status(request_id: UUID, service: str = Depends(principal)):
        return await RequestService(app.state.sessions).get_status(service, request_id)

    async def operator(service: str = Depends(principal)):
        if service != 'operator':
            raise HTTPException(403, 'Operator credentials required')

    @app.post('/api/v1/requests/{request_id}/retry', status_code=202, dependencies=[Depends(operator)])
    async def retry_request(request_id: UUID, body: RetryInput):
        return await Operations(app.state.sessions).retry('prepare', request_id, body.retry_id)

    @app.post('/api/v1/deliveries/{delivery_id}/retry', status_code=202, dependencies=[Depends(operator)])
    async def retry_delivery(delivery_id: UUID, body: RetryInput):
        return await Operations(app.state.sessions).retry('send', delivery_id, body.retry_id)

    @app.get('/api/v1/operations/failures', dependencies=[Depends(operator)])
    async def failures(limit: int = Query(100, ge=1, le=100), offset: int = Query(0, ge=0)):
        return await Operations(app.state.sessions).failures(limit, offset)

    @app.get('/api/v1/operations/backlog', dependencies=[Depends(operator)])
    async def backlog():
        return await Operations(app.state.sessions).backlog()
    return app


app = create_app()
