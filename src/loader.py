from datetime import UTC, datetime

import pandas as pd
from sqlalchemy.orm import Session

from .models import SpimexTradingResults


class SpimexLoader:
    def __init__(self, session: Session, df: pd.DataFrame | None = None) -> None:
        self.session = session
        self.df = df
        self.model = SpimexTradingResults
        if self.df is None:
            raise ValueError("[Loader] DataFrame для загрузки отсутствуют.")

    def load(self, update_on_conflict: bool = False) -> None:
        model_columns = {c.name for c in self.model.__table__.columns}

        df_filtered = self.df.loc[:, self.df.columns.intersection(model_columns)]  # type: ignore
        df_filtered["date"] = pd.to_datetime(df_filtered["date"])  # type: ignore

        now = datetime.now(UTC)
        df_filtered["created_on"] = now
        df_filtered["updated_on"] = now

        df_filtered = df_filtered.where(pd.notnull(df_filtered), None)  # type: ignore
        print(f"[Loader] Получено {len(df_filtered)} строк.")  # type: ignore

        records = df_filtered.to_dict(orient="records")  # type: ignore

        if update_on_conflict:
            for record in records:  # type: ignore
                self.session.merge(self.model(**record))  # type: ignore
                self.session.commit()
        else:
            objects = [self.model(**record) for record in records]  # type: ignore
            self.session.add_all(objects)  # type: ignore
            self.session.commit()
            print(f"[Loader] Загружено {len(objects)} строк.")
