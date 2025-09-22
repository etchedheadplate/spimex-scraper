from fastapi import APIRouter, Depends
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.db_depends import get_async_db
from src.api.schemas import TradingDates, TradingResults
from src.database.models import SpimexTradingResults as TradingModel

service_router = APIRouter(tags=["service"])


@service_router.get("/")
async def root():
    return RedirectResponse(url="/docs")


@service_router.get("/ping")
async def ping():
    return {"Response": "pong"}


trades_router = APIRouter(prefix="/trades", tags=["trades"])


@trades_router.get("/dates", response_model=TradingDates)
async def get_last_trading_dates(days: int, db: AsyncSession = Depends(get_async_db)):
    result = await db.scalars(select(TradingModel.date).distinct().order_by(TradingModel.date.desc()).limit(days))
    dates = result.all()
    return {"dates": dates}


@trades_router.get("/dynamics")
async def get_dynamics():
    pass


@trades_router.get("/results", response_model=list[TradingResults])
async def get_trading_results(db: AsyncSession = Depends(get_async_db)):
    result = await db.scalars(select(TradingModel))
    trades = result.all()
    return trades
