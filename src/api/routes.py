from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.db_depends import get_async_db
from src.api.schemas import TradingResults
from src.database.models import SpimexTradingResults as TradingModel

router = APIRouter(prefix="/trades", tags=["trades"])


@router.get("/dates")
async def get_last_trading_dates():
    pass


@router.get("/dynamics")
async def get_dynamics():
    pass


@router.get("/results", response_model=list[TradingResults])
async def get_trading_results(db: AsyncSession = Depends(get_async_db)):
    result = await db.scalars(select(TradingModel))
    trades = result.all()
    return trades
