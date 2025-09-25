import json
from collections.abc import AsyncGenerator
from datetime import date, timedelta
from typing import Any

import redis.asyncio as redis
from fastapi import Query, Request
from pydantic import PositiveInt
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.schemas import (
    LastTradingDatesQuery,
    TradingDynamicsQuery,
    TradingResultsQuery,
)
from src.database.connection import async_session_maker

redis_client: redis.Redis = redis.Redis(host="127.0.0.1", port=6379, db=0, encoding="utf-8", decode_responses=True)


def get_cache_key(request: Request) -> str:
    return f"cache:{request.url.path}?{request.url.query}"


async def get_from_cache(request: Request):
    key = get_cache_key(request)
    data = await redis_client.get(key)
    if data:
        return json.loads(data)
    return None


async def set_cache(request: Request, response_data: Any):
    key = get_cache_key(request)
    await redis_client.set(key, json.dumps(response_data))


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session


def last_trading_days_query(
    days: PositiveInt = Query(..., description="Количество торговых дней", example=5)
) -> LastTradingDatesQuery:
    return LastTradingDatesQuery(days=days)


def trading_dynamics_query(
    start_date: date = Query(..., description="Начало периода", example=(date.today() - timedelta(days=7)).isoformat()),
    end_date: date = Query(..., description="Конец периода", example=date.today().isoformat()),
    oil_id: str | None = Query(
        None, min_length=4, max_length=4, pattern="^[A-Z0-9]{4}$", description="Код биржевого товара", example="A692"
    ),
    delivery_type_id: str | None = Query(
        None, min_length=1, max_length=1, pattern="^[A-Z0-9]$", description="Условие поставки", example="A"
    ),
    delivery_basis_id: str | None = Query(
        None, min_length=3, max_length=3, pattern="^[A-Z0-9]{3}$", description="Код базиса поставки", example="ACH"
    ),
) -> TradingDynamicsQuery:
    return TradingDynamicsQuery(
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id,
        start_date=start_date,
        end_date=end_date,
    )


def trading_results_query(
    oil_id: str | None = Query(
        None, min_length=4, max_length=4, pattern="^[A-Z0-9]{4}$", description="Код биржевого товара", example="A692"
    ),
    delivery_type_id: str | None = Query(
        None, min_length=1, max_length=1, pattern="^[A-Z0-9]$", description="Условие поставки", example="A"
    ),
    delivery_basis_id: str | None = Query(
        None, min_length=3, max_length=3, pattern="^[A-Z0-9]{3}$", description="Код базиса поставки", example="ACH"
    ),
) -> TradingResultsQuery:
    return TradingResultsQuery(
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id,
    )
