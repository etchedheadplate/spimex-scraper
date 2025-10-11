from collections.abc import AsyncGenerator
from datetime import date

from fastapi import Query
from pydantic import PositiveInt
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.schemas import (
    LastTradingDatesQuery,
    TradingDynamicsQuery,
    TradingResultsQuery,
)
from src.database.connection import async_session_maker


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session


def last_trading_days_query(
    days: PositiveInt = Query(..., description="Количество торговых дней")
) -> LastTradingDatesQuery:
    return LastTradingDatesQuery(days=days)


def trading_dynamics_query(
    start_date: date = Query(..., description="Начало периода"),
    end_date: date = Query(..., description="Конец периода"),
    oil_id: str | None = Query(
        None, min_length=4, max_length=4, pattern="^[A-Z0-9]{4}$", description="Код биржевого товара"
    ),
    delivery_type_id: str | None = Query(
        None, min_length=1, max_length=1, pattern="^[A-Z0-9]$", description="Условие поставки"
    ),
    delivery_basis_id: str | None = Query(
        None, min_length=3, max_length=3, pattern="^[A-Z0-9]{3}$", description="Код базиса поставки"
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
        None, min_length=4, max_length=4, pattern="^[A-Z0-9]{4}$", description="Код биржевого товара"
    ),
    delivery_type_id: str | None = Query(
        None, min_length=1, max_length=1, pattern="^[A-Z0-9]$", description="Условие поставки"
    ),
    delivery_basis_id: str | None = Query(
        None, min_length=3, max_length=3, pattern="^[A-Z0-9]{3}$", description="Код базиса поставки"
    ),
) -> TradingResultsQuery:
    return TradingResultsQuery(
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id,
    )
