# pyright: basic

from datetime import datetime
from typing import cast

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from .models import SpimexTradingResults


class SpimexLoader:
    def __init__(
        self,
        session: AsyncSession,
        df: pd.DataFrame | None = None,
        update_on_conflict: bool = False,
        chunk_size: int = 1000,
    ) -> None:
        self.session = session
        self.df = df
        self.update_on_conflict = update_on_conflict
        self.chunk_size = chunk_size
        self.model = SpimexTradingResults
        if df is None:
            raise ValueError("[Loader] DataFrame для загрузки отсутствует.")

    async def load(self) -> None:
        model_columns = {c.name for c in self.model.__table__.columns}

        df = cast(pd.DataFrame, self.df)
        df_filtered = df.loc[:, df.columns.intersection(model_columns)]
        df_filtered["date"] = pd.to_datetime(df_filtered["date"]).dt.date

        now = datetime.now()
        df_filtered["created_on"] = now
        df_filtered["updated_on"] = now

        df_filtered = df_filtered.where(pd.notnull(df_filtered), None)
        total_rows = len(df_filtered)
        print(f"[Loader] Получено {total_rows} строк для загрузки.")

        records = df_filtered.to_dict(orient="records")

        total_processed = 0
        for i in range(0, len(records), self.chunk_size):
            chunk = records[i : i + self.chunk_size]
            chunk_size_actual = len(chunk)

            try:
                if self.update_on_conflict:
                    for record in chunk:
                        await self.session.merge(self.model(**record))
                else:
                    objects = [self.model(**record) for record in chunk]
                    self.session.add_all(objects)

                await self.session.commit()
                total_processed += chunk_size_actual
                print(
                    f"[Loader] Загружен чанк {i//self.chunk_size + 1}: {chunk_size_actual} строк."
                    f"Всего обработано: {total_processed}/{total_rows} ({total_processed/total_rows*100:.1f}%)"
                )

            except Exception as e:
                await self.session.rollback()
                print(f"[Loader] Ошибка при загрузке чанка {i//self.chunk_size + 1}: {e}")
                raise

        print(f"[Loader] Успешно загружено {total_processed} строк.")
