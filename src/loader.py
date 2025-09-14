import pandas as pd
from sqlalchemy.orm import Session

from .models import SpimexTradingResults


class SpimexLoader:
    def __init__(self, session: Session):
        self.model = SpimexTradingResults
        self.session = session

    def load(self, df: pd.DataFrame, update_on_conflict: bool = False) -> None:
        model_columns = {c.name for c in self.model.__table__.columns}

        df_filtered = df.loc[:, df.columns.intersection(model_columns)]  # type: ignore
        df_filtered = df_filtered.where(pd.notnull(df_filtered), None)  # type: ignore

        records = df_filtered.to_dict(orient="records")  # type: ignore

        if update_on_conflict:
            for record in records:  # type: ignore
                self.session.merge(self.model(**record))  # type: ignore
                self.session.commit()
        else:
            objects = [self.model(**record) for record in records]  # type: ignore
            self.session.add_all(objects)  # type: ignore
            self.session.commit()
