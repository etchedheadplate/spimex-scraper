from fastapi import FastAPI

from src.api.routes import trades_router

app = FastAPI(title="Spimex Trading Results API")

app.include_router(trades_router)


if __name__ == "__main__":
    import asyncio

    from src.workers.updater import update_database

    asyncio.run(update_database())
