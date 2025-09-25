import asyncio

from src.processing.db_updater import update_database

if __name__ == "__main__":
    asyncio.run(update_database())
