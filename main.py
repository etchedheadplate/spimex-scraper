# import asyncio
import uvicorn
from fastapi import FastAPI
from fastapi.responses import RedirectResponse

# from src.workers.updater import update_database
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
    uvicorn.run("main:app", port=8000, host="127.0.0.1", reload=True)
#    asyncio.run(update_database())
