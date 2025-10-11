from datetime import date

from fastapi import APIRouter, Depends, Request
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import (
    get_async_db,
    last_trading_days_query,
    trading_dynamics_query,
    trading_results_query,
)
from src.api.schemas import (
    LastTradingDatesQuery,
    LastTradingDatesSchema,
    TradingDynamicsQuery,
    TradingDynamicsSchema,
    TradingResultsQuery,
    TradingResultsSchema,
)
from src.cache import get_from_cache, set_cache
from src.database.models import SpimexTradingResults as TradingModel

trades_router = APIRouter(prefix="/trades", tags=["trades"])


@trades_router.get("/ping", name="ping")
async def ping():
    return {"status": "ok"}


@trades_router.get(
    "/dates",
    response_model=LastTradingDatesSchema,
    summary="Список дат последних торговых дней",
    description="Возвращает список дат, в которые велись торги",
    name="get_dates",
)
async def get_last_trading_dates(
    request: Request,
    query: LastTradingDatesQuery = Depends(last_trading_days_query),
    db: AsyncSession = Depends(get_async_db),
):
    cached = await get_from_cache(request)
    if cached:
        cached_data = [date.fromisoformat(d) for d in cached]
        print(f"Got from cache {len(cached_data)} items")
        return {"dates": cached_data, "cached": True}

    stmt = select(TradingModel.date).distinct().order_by(TradingModel.date.desc()).limit(query.days)
    result = await db.scalars(stmt)
    result_data: list[date] = list(result.all())
    data_to_cache = [d.isoformat() for d in result_data]

    await set_cache(request, data_to_cache)
    print(f"Set to cache {len(data_to_cache)} items")
    return LastTradingDatesSchema(dates=result_data)


@trades_router.get(
    "/dynamics",
    response_model=list[TradingDynamicsSchema],
    summary="Список торгов за заданный период",
    description="Возвращает список торговых позиций с включением начальной и конечной дат периода",
    name="get_dynamics",
)
async def get_dynamics(
    request: Request,
    query: TradingDynamicsQuery = Depends(trading_dynamics_query),
    db: AsyncSession = Depends(get_async_db),
):
    cached = await get_from_cache(request)
    if cached:
        cached_data = [TradingDynamicsSchema.model_validate(item) for item in cached]
        print(f"Got from cache {len(cached_data)} items")
        return cached_data

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
    result_data = result.all()

    data_to_cache = [
        {
            k: (v.isoformat() if isinstance(v, date) else v)
            for k, v in TradingDynamicsSchema.model_validate(item).model_dump().items()
        }
        for item in result_data
    ]

    await set_cache(request, data_to_cache)
    print(f"Set to cache {len(data_to_cache)} items")
    return [TradingDynamicsSchema.model_validate(item) for item in result_data]


@trades_router.get(
    "/results",
    response_model=list[TradingResultsSchema],
    summary="Список последних торгов",
    description="Возвращает список с параметрами торговой позиции за последний торговый день",
    name="get_results",
)
async def get_trading_results(
    request: Request,
    query: TradingResultsQuery = Depends(trading_results_query),
    db: AsyncSession = Depends(get_async_db),
):
    cached = await get_from_cache(request)
    if cached:
        cached_data = [TradingResultsSchema.model_validate(item) for item in cached]
        print(f"Got from cache {len(cached_data)} items")
        return cached_data

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
    result_data = result.all()

    data_to_cache = [
        {
            k: (v.isoformat() if isinstance(v, date) else v)
            for k, v in TradingResultsSchema.model_validate(item).model_dump().items()
        }
        for item in result_data
    ]

    await set_cache(request, data_to_cache)
    print(f"Set to cache {len(data_to_cache)} items")
    return [TradingResultsSchema.model_validate(item) for item in result_data]
