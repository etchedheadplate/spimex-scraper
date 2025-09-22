import asyncio

from src.workers.updater import update_database

if __name__ == "__main__":
    asyncio.run(update_database())
