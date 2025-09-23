from datetime import date

from fastapi import APIRouter, Depends
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.db_depends import get_async_db
from src.api.schemas import (
    LastTradingDatesQuery,
    LastTradingDatesSchema,
    TradingDynamicsQuery,
    TradingDynamicsSchema,
    TradingResultsQuery,
    TradingResultsSchema,
)
from src.database.models import SpimexTradingResults as TradingModel

trades_router = APIRouter(prefix="/trades", tags=["trades"])


@trades_router.get("/dates", response_model=LastTradingDatesSchema)
async def get_last_trading_dates(query: LastTradingDatesQuery = Depends(), db: AsyncSession = Depends(get_async_db)):
    stmt = select(TradingModel.date).distinct().order_by(TradingModel.date.desc()).limit(query.days)
    result = await db.scalars(stmt)
    dates_only: list[date] = list(result.all())
    return LastTradingDatesSchema(dates=dates_only)


@trades_router.get("/dynamics", response_model=list[TradingDynamicsSchema])
async def get_dynamics(
    query: TradingDynamicsQuery = Depends(),
    db: AsyncSession = Depends(get_async_db),
):
    stmt = select(TradingModel).where(
        and_(
            TradingModel.oil_id == query.oil_id,
            TradingModel.delivery_type_id == query.delivery_type_id,
            TradingModel.delivery_basis_id == query.delivery_basis_id,
            TradingModel.date >= query.start_date,
            TradingModel.date <= query.end_date,
        )
    )

    result = await db.scalars(stmt)
    return result.all()


@trades_router.get("/results", response_model=list[TradingResultsSchema])
async def get_trading_results(
    query: TradingResultsQuery = Depends(),
    db: AsyncSession = Depends(get_async_db),
):
    latest_date = await db.scalar(select(func.max(TradingModel.date)))

    stmt = select(TradingModel).where(
        and_(
            TradingModel.date == latest_date,
            TradingModel.oil_id == query.oil_id,
            TradingModel.delivery_type_id == query.delivery_type_id,
            TradingModel.delivery_basis_id == query.delivery_basis_id,
        )
    )
    result = await db.scalars(stmt)
    return result.all()
