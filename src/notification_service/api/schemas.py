from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, AwareDatetime, HttpUrl, TypeAdapter, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RegistrationPayload(StrictModel):
    user_id: UUID


class MoviePayload(StrictModel):
    movie_id: UUID
    title: str = Field(min_length=1, max_length=300)
    url: HttpUrl


class RegistrationEvent(StrictModel):
    channel: Literal["email"] = "email"
    event_id: UUID
    event_type: Literal["user.registered"]
    occurred_at: AwareDatetime
    payload: RegistrationPayload


class MovieEvent(StrictModel):
    channel: Literal["email"] = "email"
    event_id: UUID
    event_type: Literal["movie.published"]
    occurred_at: AwareDatetime
    payload: MoviePayload


EventInput = Annotated[RegistrationEvent | MovieEvent, Field(discriminator="event_type")]
event_adapter = TypeAdapter(EventInput)


def parse_event(data):
    return event_adapter.validate_python(data)


class UsersAudience(StrictModel):
    kind: Literal["users"]
    user_ids: list[UUID] = Field(min_length=1, max_length=1000)

    @field_validator("user_ids")
    @classmethod
    def normalize(cls, ids):
        return sorted(set(ids), key=str)


class AllAudience(StrictModel):
    kind: Literal["all"]


class CampaignData(StrictModel):
    headline: str = Field(min_length=1, max_length=200)
    message: str = Field(min_length=1, max_length=10000)

    @field_validator("headline")
    @classmethod
    def single_line(cls, value):
        if "\r" in value or "\n" in value:
            raise ValueError("headline must be one line")
        return value


class CampaignInput(StrictModel):
    channel: Literal["email"] = "email"
    priority: Literal["bulk", "urgent"] = "bulk"
    request_id: UUID
    template_id: Literal["announcement", "personal_announcement"]
    audience: Annotated[UsersAudience | AllAudience, Field(discriminator="kind")]
    data: CampaignData

    @model_validator(mode="after")
    def bounded_urgent(self):
        if self.priority == "urgent" and (not isinstance(self.audience, UsersAudience) or len(self.audience.user_ids) > 100):
            raise ValueError("Urgent campaigns require at most 100 explicit recipients")
        return self


class BrokerEnvelope(StrictModel):
    lane: Literal["bulk", "urgent"] = "bulk"
    schema_version: Literal[1] = 1
    message_id: UUID
    kind: Literal["prepare", "send", "failed"]
    entity_id: UUID
    generation: int = Field(ge=0)
    error_code: str | None = None
