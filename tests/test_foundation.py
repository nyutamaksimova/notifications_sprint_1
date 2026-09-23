from uuid import uuid4

import pytest
from notification_service.config import Settings
from notification_service.db.models import Base, Delivery, Request, Template
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


async def test_delivery_uniqueness_and_atomic_rollback():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with async_sessionmaker(engine, expire_on_commit=False)() as session:
        template = Template(template_id="welcome", version=1, subject="Hi", html_body="Hi", text_body="Hi")
        session.add(template)
        await session.flush()
        request = Request(service_id="auth", endpoint_kind="events", external_id=uuid4(),
                          input_hash="hash", template_version_id=template.id, audience={}, data={})
        session.add(request)
        await session.flush()
        user_id = uuid4()
        session.add(Delivery(request_id=request.id, user_id=user_id))
        await session.commit()
        session.add(Delivery(request_id=request.id, user_id=user_id))
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()
    await engine.dispose()


def test_configuration_has_no_working_secret_defaults():
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
