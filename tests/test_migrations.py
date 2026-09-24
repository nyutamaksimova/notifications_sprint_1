import hashlib
import json
import os
import subprocess
import sys
from uuid import UUID, uuid4

from notification_service.api.schemas import parse_event
from notification_service.db.models import Request, now
from notification_service.requests import RequestService
from sqlalchemy import MetaData, Table
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine


async def test_initial_migration_creates_delivery_constraints(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'migration.db'}"
    result = subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"],
                            env={**os.environ, "DATABASE_URL": url}, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    engine = create_async_engine(url)
    async with engine.connect() as connection:
        constraints = await connection.run_sync(lambda c: inspect(c).get_unique_constraints("deliveries"))
        indexes = await connection.run_sync(lambda c: inspect(c).get_indexes("outbox"))
    assert any(set(item["column_names"]) == {"request_id", "user_id"} for item in constraints)
    assert any(item["name"] == "ix_outbox_pending_route_time" for item in indexes)
    await engine.dispose()


async def test_upgrade_preserves_old_request_hash_and_bulk_lane(tmp_path, event):
    url = f"sqlite+aiosqlite:///{tmp_path / 'upgrade.db'}"

    def migrate(revision):
        result = subprocess.run([sys.executable, '-m', 'alembic', 'upgrade', revision],
                                env={**os.environ, 'DATABASE_URL': url}, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr

    migrate('0001')
    request_id, template_id = uuid4(), uuid4()
    old_body = parse_event(event).model_dump(mode='json', exclude={'channel'})
    digest = hashlib.sha256(json.dumps(old_body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    engine = create_async_engine(url)
    async with engine.begin() as connection:
        def insert_old(sync):
            metadata = MetaData()
            templates = Table('templates', metadata, autoload_with=sync)
            requests = Table('requests', metadata, autoload_with=sync)
            sync.execute(templates.insert().values(id=template_id.hex, template_id='welcome', version=1,
                                                   subject='Hi', html_body='Hi', text_body='Hi', created_at=now()))
            sync.execute(requests.insert().values(id=request_id.hex, service_id='auth', endpoint_kind='events',
                                                  external_id=UUID(event['event_id']).hex, input_hash=digest,
                                                  event_type='user.registered',
                                                  template_version_id=template_id.hex,
                                                  audience={'kind': 'users', 'user_ids': [event['payload']['user_id']]},
                                                  data={}, accepted_at=now(), audience_cutoff=now(), status='accepted',
                                                  attempts=0,
                                                  generation=0, next_attempt_at=now()))

        await connection.run_sync(insert_old)
    await engine.dispose()
    migrate('head')
    engine = create_async_engine(url)
    try:
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        receipt = await RequestService(sessions).accept('auth', 'events', parse_event({**event, 'channel': 'email'}))
        assert receipt['id'] == str(request_id)
        async with sessions() as session:
            row = await session.get(Request, request_id)
            assert (row.lane, row.channel, row.category) == ('bulk', 'email', 'account')
    finally:
        await engine.dispose()
