# pyright: basic

import asyncio
from datetime import datetime
from typing import cast

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database.models import SpimexTradingResults


class SpimexLoader:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        df: pd.DataFrame | None = None,
        update_on_conflict: bool = False,
        chunk_size: int = 1000,
        max_parallel_chunks: int = 5,
    ) -> None:
        self.sessionmaker = sessionmaker
        self.df = df
        self.update_on_conflict = update_on_conflict
        self.chunk_size = chunk_size
        self.max_parallel_chunks = max_parallel_chunks
        self.model = SpimexTradingResults
        try:
            if df is None:
                raise ValueError("[Loader] DataFrame для загрузки отсутствует.")
        except ValueError as e:
            print(e)
            return
        except Exception as e:
            print(f"[Loader] Ошибка при загрузке данных: {e}")
            return

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

        sem = asyncio.Semaphore(self.max_parallel_chunks)

        async def process_chunk(idx: int, chunk: list[dict]) -> int:
            async with sem, self.sessionmaker() as session:
                try:
                    if self.update_on_conflict:
                        for record in chunk:
                            await session.merge(self.model(**record))
                    else:
                        objects = [self.model(**record) for record in chunk]
                        session.add_all(objects)

                    await session.commit()
                    print(f"[Loader] Загружен чанк {idx + 1}: {len(chunk)} строк.")
                    return len(chunk)
                except Exception as e:
                    await session.rollback()
                    print(f"[Loader] Ошибка при загрузке чанка {idx + 1}: {e}")
                    raise

        tasks = []
        for row in range(0, len(records), self.chunk_size):
            chunk = records[row : row + self.chunk_size]
            tasks.append(process_chunk(row // self.chunk_size, chunk))

        results = await asyncio.gather(*tasks)
        total_processed = sum(results)

        print(f"[Loader] Успешно загружено {total_processed} строк.")
