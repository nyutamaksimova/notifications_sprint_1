import asyncio
from notification_service.workers.runtime import run

if __name__ == "__main__":
    asyncio.run(run("publisher"))

