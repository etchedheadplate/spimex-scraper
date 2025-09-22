from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from src.api.routes import router

app = FastAPI(title="Spimex API")

app.include_router(router)


@app.get("/")
async def root():
    return RedirectResponse(url="/docs")


@app.get("/ping")
async def ping():
    return {"Response": "pong"}


if __name__ == "__main__":
    import asyncio

    from src.workers.updater import update_database

    asyncio.run(update_database())
