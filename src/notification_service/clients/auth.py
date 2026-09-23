from uuid import UUID
from pydantic import BaseModel, EmailStr
import httpx
from datetime import UTC
from notification_service.retry import TemporaryError, PermanentError, RecipientMissing


class Recipient(BaseModel):
    id: UUID
    email: EmailStr | None
    first_name: str | None = None
    last_name: str | None = None


class UserPage(BaseModel):
    items: list[UUID]
    next_cursor: str | None = None


class AuthClient:
    def __init__(self, base_url, token, http):
        self.base_url, self.token, self.http = base_url.rstrip('/'), token, http

    async def get(self, path, params=None):
        try:
            response = await self.http.get(self.base_url + path, params=params,
                headers={"Authorization": "Bearer " + self.token}, timeout=5)
        except httpx.HTTPError as error:
            raise TemporaryError("auth_network") from error
        if response.status_code == 404:
            raise RecipientMissing()
        if response.status_code == 429 or response.status_code >= 500:
            delay = response.headers.get("Retry-After", "")
            raise TemporaryError("auth_temporary", int(delay) if delay.isdigit() else None)
        if response.status_code != 200:
            raise PermanentError("auth_rejected")
        try:
            return response.json()
        except ValueError as error:
            raise PermanentError("auth_invalid_response") from error

    async def get_recipient(self, user_id):
        try:
            recipient = Recipient.model_validate(await self.get(f"/internal/users/{user_id}"))
            if recipient.id != user_id:
                raise ValueError("wrong recipient")
            return recipient
        except ValueError as error:
            raise PermanentError("auth_invalid_response") from error

    async def list_user_ids(self, limit, cursor, created_before):
        params = {"limit": limit, "created_before": created_before.replace(tzinfo=UTC).isoformat()}
        if cursor:
            params["cursor"] = cursor
        try:
            data = await self.get("/internal/users", params)
            return UserPage(items=[item["id"] for item in data["items"]], next_cursor=data["next_cursor"])
        except (ValueError, KeyError, TypeError, RecipientMissing) as error:
            raise PermanentError("auth_invalid_page") from error
