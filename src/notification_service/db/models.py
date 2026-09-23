from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, Index, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def now():
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Template(Base):
    __tablename__ = "templates"
    __table_args__ = (UniqueConstraint("template_id", "version"),)
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    template_id: Mapped[str] = mapped_column(String(100))
    version: Mapped[int]
    subject: Mapped[str] = mapped_column(Text)
    html_body: Mapped[str] = mapped_column(Text)
    text_body: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class WorkState:
    channel: Mapped[str] = mapped_column(String(20), default="email")
    lane: Mapped[str] = mapped_column(String(20), default="bulk", index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    attempts: Mapped[int] = mapped_column(default=0)
    generation: Mapped[int] = mapped_column(default=0)
    lease_token: Mapped[UUID | None]
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    last_error_code: Mapped[str | None] = mapped_column(String(80))


class Request(WorkState, Base):
    category: Mapped[str] = mapped_column(String(20), default="marketing")
    __tablename__ = "requests"
    __table_args__ = (UniqueConstraint("service_id", "endpoint_kind", "external_id"),)
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    service_id: Mapped[str] = mapped_column(String(50))
    endpoint_kind: Mapped[str] = mapped_column(String(20))
    external_id: Mapped[UUID]
    input_hash: Mapped[str] = mapped_column(String(64))
    event_type: Mapped[str | None] = mapped_column(String(100))
    template_version_id: Mapped[UUID] = mapped_column(ForeignKey("templates.id"))
    audience: Mapped[dict] = mapped_column(JSON)
    data: Mapped[dict] = mapped_column(JSON)
    accepted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    audience_cutoff: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    cursor: Mapped[str | None] = mapped_column(Text)


class Delivery(WorkState, Base):
    __tablename__ = "deliveries"
    __table_args__ = (UniqueConstraint("request_id", "user_id"), Index("ix_delivery_status", "request_id", "status"))
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    request_id: Mapped[UUID] = mapped_column(ForeignKey("requests.id"))
    user_id: Mapped[UUID]
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Outbox(Base):
    __tablename__ = "outbox"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    routing_key: Mapped[str] = mapped_column(String(20))
    payload: Mapped[dict] = mapped_column(JSON)
    dedup_key: Mapped[str] = mapped_column(String(200), unique=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now, index=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(default=0)
    lease_token: Mapped[UUID | None]
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Preference(Base):
    __tablename__ = "preferences"
    user_id: Mapped[UUID] = mapped_column(primary_key=True)
    settings: Mapped[dict] = mapped_column(JSON)


class RetryCommand(Base):
    __tablename__ = "retry_commands"
    kind: Mapped[str] = mapped_column(String(20), primary_key=True)
    entity_id: Mapped[UUID] = mapped_column(primary_key=True)
    retry_id: Mapped[UUID] = mapped_column(primary_key=True)
    generation: Mapped[int]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
