from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class TradingDates(BaseModel):
    dates: list[date]

    model_config = ConfigDict(from_attributes=True)


class TradingDynamics(BaseModel):
    oil_id: str = Field(description="Продукт")
    delivery_type_id: str = Field(description="Тип поставки")
    delivery_basis_id: str = Field(description="Базис поставки")
    start_date: str = Field(description="Начало периода")
    end_date: str = Field(description="Конец периода")

    model_config = ConfigDict(from_attributes=True)


class TradingResults(BaseModel):
    oil_id: str = Field(description="Продукт")
    delivery_type_id: str = Field(description="Тип поставки")
    delivery_basis_id: str = Field(description="Базис поставки")

    model_config = ConfigDict(from_attributes=True)
