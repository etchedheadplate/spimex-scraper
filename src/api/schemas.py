from datetime import date
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, PositiveInt


class LastTradingDatesSchema(BaseModel):
    dates: list[date]


class LastTradingDatesQuery(BaseModel):
    days: PositiveInt = Field(..., description="Количество дней")


class TradingDynamicsSchema(BaseModel):
    exchange_product_id: str
    oil_id: str
    delivery_basis_id: str
    delivery_basis_name: str
    delivery_type_id: str
    volume: int
    total: int
    count: int
    date: date

    model_config = ConfigDict(from_attributes=True)


class TradingDynamicsQuery(BaseModel):
    oil_id: Annotated[str, Field(min_length=4, max_length=4, pattern="^[A-Z0-9]{4}$")] = Field(
        ..., description="код биржевого товара"
    )
    delivery_type_id: Annotated[str, Field(min_length=1, max_length=1, pattern="^[A-Z0-9]$")] = Field(
        ..., description="Условие поставки"
    )
    delivery_basis_id: Annotated[str, Field(min_length=3, max_length=3, pattern="^[A-Z0-9]{3}$")] = Field(
        ..., description="Код базиса поставки"
    )
    start_date: date = Field(..., description="Начало периода")
    end_date: date = Field(..., description="Конец периода")


class TradingResultsSchema(BaseModel):
    exchange_product_id: str
    oil_id: str
    delivery_basis_id: str
    delivery_basis_name: str
    delivery_type_id: str
    volume: int
    total: int
    count: int
    date: date

    model_config = ConfigDict(from_attributes=True)


class TradingResultsQuery(BaseModel):
    oil_id: Annotated[str, Field(min_length=4, max_length=4, pattern="^[A-Z0-9]{4}$")] = Field(
        ..., description="Код биржевого товара"
    )
    delivery_type_id: Annotated[str, Field(min_length=1, max_length=1, pattern="^[A-Z0-9]$")] = Field(
        ..., description="Условие поставки"
    )
    delivery_basis_id: Annotated[str, Field(min_length=3, max_length=3, pattern="^[A-Z0-9]{3}$")] = Field(
        ..., description="Код базиса поставки"
    )
