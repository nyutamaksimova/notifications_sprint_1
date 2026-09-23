import asyncio
from notification_service.config import Settings
from notification_service.db.session import session_factory
from notification_service.templates import seed_templates


async def main():
    sessions = session_factory(Settings())
    try:
        async with sessions() as session:
            await seed_templates(session)
    finally:
        await sessions.kw["bind"].dispose()


if __name__ == "__main__":
    asyncio.run(main())

