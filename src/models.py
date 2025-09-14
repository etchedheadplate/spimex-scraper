from datetime import datetime

from sqlalchemy import Date, DateTime, Integer, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BaseModel(DeclarativeBase):
    pass


class SpimexTradingResults(BaseModel):
    __tablename__ = "spimex_trading_results"

    now = datetime.now()

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange_product_id: Mapped[str] = mapped_column(String(20), nullable=False)
    oil_id: Mapped[str] = mapped_column(String(10), nullable=False)
    delivery_basis_id: Mapped[str] = mapped_column(String(10), nullable=False)
    delivery_basis_name: Mapped[str] = mapped_column(String(250), nullable=False)
    delivery_type_id: Mapped[str] = mapped_column(String(10), nullable=False)
    volume: Mapped[int] = mapped_column(Integer, nullable=False)
    total: Mapped[int] = mapped_column(Integer, nullable=False)
    count: Mapped[int] = mapped_column(Integer, nullable=False)
    date: Mapped[datetime] = mapped_column(Date, nullable=True)
    created_on: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_on: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    def __repr__(self):
        fields = [
            f"id={self.id}",
            f"exchange_product_id='{self.exchange_product_id}'",
            f"oil_id='{self.oil_id}'",
            f"delivery_basis_id='{self.delivery_basis_id}'",
            f"delivery_basis_name='{self.delivery_basis_name}'",
            f"delivery_type_id='{self.delivery_type_id}'",
            f"volume={self.volume}",
            f"total={self.total}",
            f"count={self.count}",
            f"date={self.date}",
            f"created_on={self.created_on}",
            f"updated_on={self.updated_on}",
        ]
        return f"<SpimexTradingResults({', '.join(fields)})>"
