from datetime import UTC, datetime

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from .models import SpimexTradingResults


class SpimexLoader:
    def __init__(self, session: AsyncSession, df: pd.DataFrame | None = None) -> None:
        self.session = session
        self.df = df
        self.model = SpimexTradingResults
        if self.df is None:
            raise ValueError("[Loader] DataFrame для загрузки отсутствует.")

    async def load(self, update_on_conflict: bool = False) -> None:
        model_columns = {c.name for c in self.model.__table__.columns}

        df_filtered = self.df.loc[:, self.df.columns.intersection(model_columns)]  # type: ignore
        df_filtered["date"] = pd.to_datetime(df_filtered["date"]).dt.date  # type: ignore

        now = datetime.now(UTC)
        df_filtered["created_on"] = now
        df_filtered["updated_on"] = now

        df_filtered = df_filtered.where(pd.notnull(df_filtered), None)  # type: ignore
        print(f"[Loader] Получено {len(df_filtered)} строк.")  # type: ignore

        records = df_filtered.to_dict(orient="records")  # type: ignore

        if update_on_conflict:
            for record in records:  # type: ignore
                await self.session.merge(self.model(**record))
            await self.session.commit()
        else:
            objects = [self.model(**record) for record in records]  # type: ignore
            self.session.add_all(objects)
            await self.session.commit()
            print(f"[Loader] Загружено {len(objects)} строк.")
