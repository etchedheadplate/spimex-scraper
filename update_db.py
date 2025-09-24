if __name__ == "__main__":
    import asyncio

    from src.processing.updater import update_database

    asyncio.run(update_database())
