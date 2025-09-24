from fastapi import APIRouter, FastAPI

from src.api.routes import trades_router

app = FastAPI(title="Spimex API")

api_v1 = APIRouter(prefix="/v1")
api_v1.include_router(trades_router)

app.include_router(api_v1)

if __name__ == "__main__":
    import asyncio

    from src.workers.updater import update_database

    asyncio.run(update_database())
