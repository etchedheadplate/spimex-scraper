from datetime import date

from fastapi import APIRouter, Depends
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_async_db, last_trading_days_query, trading_dynamics_query, trading_results_query
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


@trades_router.get(
    "/dates",
    response_model=LastTradingDatesSchema,
    summary="Список дат последних торговых дней",
    description="Возвращает список дат, в которые велись торги",
)
async def get_last_trading_dates(
    query: LastTradingDatesQuery = Depends(last_trading_days_query), db: AsyncSession = Depends(get_async_db)
):
    stmt = select(TradingModel.date).distinct().order_by(TradingModel.date.desc()).limit(query.days)
    result = await db.scalars(stmt)
    dates_only: list[date] = list(result.all())
    return LastTradingDatesSchema(dates=dates_only)


@trades_router.get(
    "/dynamics",
    response_model=list[TradingDynamicsSchema],
    summary="Список торгов за заданный период",
    description="Возвращает список торговых позиций с включением начальной и конечной дат периода",
)
async def get_dynamics(
    query: TradingDynamicsQuery = Depends(trading_dynamics_query),
    db: AsyncSession = Depends(get_async_db),
):
    filters = [
        TradingModel.date >= query.start_date,
        TradingModel.date <= query.end_date,
    ]

    if query.oil_id is not None:
        filters.append(TradingModel.oil_id == query.oil_id)
    if query.delivery_type_id is not None:
        filters.append(TradingModel.delivery_type_id == query.delivery_type_id)
    if query.delivery_basis_id is not None:
        filters.append(TradingModel.delivery_basis_id == query.delivery_basis_id)

    stmt = select(TradingModel).where(and_(*filters))

    result = await db.scalars(stmt)
    return result.all()


@trades_router.get(
    "/results",
    response_model=list[TradingResultsSchema],
    summary="Список последних торгов",
    description="Возвращает список с параметрами торговой позиции за последний торговый день",
)
async def get_trading_results(
    query: TradingResultsQuery = Depends(trading_results_query),
    db: AsyncSession = Depends(get_async_db),
):
    latest_date = await db.scalar(select(func.max(TradingModel.date)))

    filters = [
        TradingModel.date == latest_date,
    ]

    if query.oil_id is not None:
        filters.append(TradingModel.oil_id == query.oil_id)
    if query.delivery_type_id is not None:
        filters.append(TradingModel.delivery_type_id == query.delivery_type_id)
    if query.delivery_basis_id is not None:
        filters.append(TradingModel.delivery_basis_id == query.delivery_basis_id)

    stmt = select(TradingModel).where(and_(*filters))

    result = await db.scalars(stmt)
    return result.all()
