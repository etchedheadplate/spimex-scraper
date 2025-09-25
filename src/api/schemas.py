from datetime import date

from pydantic import BaseModel, ConfigDict, PositiveInt


class LastTradingDatesSchema(BaseModel):
    dates: list[date]


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


class LastTradingDatesQuery(BaseModel):
    days: PositiveInt


class TradingDynamicsQuery(BaseModel):
    start_date: date
    end_date: date
    oil_id: str | None = None
    delivery_type_id: str | None = None
    delivery_basis_id: str | None = None


class TradingResultsQuery(BaseModel):
    oil_id: str | None = None
    delivery_type_id: str | None = None
    delivery_basis_id: str | None = None
