from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from notification_service.config import Settings
from notification_service.db.models import Base
from notification_service.templates import seed_templates


@pytest.fixture
async def sessions(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'test.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        await seed_templates(session)
    yield factory
    await engine.dispose()


@pytest.fixture
def settings():
    return Settings(_env_file=None, database_url="sqlite+aiosqlite:///:memory:",
                    rabbitmq_url="amqp://test:test@localhost/", auth_url="http://auth",
                    auth_reader_token="r" * 32, service_tokens={"auth": "a" * 32, "catalog": "c" * 32,
                    "operator": "o" * 32}, smtp_host="localhost")


@pytest.fixture
def event():
    return dict(event_id=str(uuid4()), event_type="user.registered",
                occurred_at="2026-09-22T10:00:00Z", payload={"user_id": str(uuid4())})
