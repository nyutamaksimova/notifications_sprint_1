from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


def session_factory(settings):
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    return async_sessionmaker(engine, expire_on_commit=False)
