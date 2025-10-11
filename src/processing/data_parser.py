# pyright: basic

from typing import Literal

import pandas as pd

from src.logger import logger


class SpimexParser:
    def __init__(
        self,
        files: list[str] | None = None,
        start_anchor: str = "Единица измерения: Метрическая тонна",
        end_anchor: str = "Итого:",
        date_anchor: str = "Дата торгов:",
        column_idx: dict[str, int] | None = None,
        engine: Literal["xlrd", "openpyxl", "odf", "pyxlsb", "calamine"] = "xlrd",
    ) -> None:
        self.files = files
        self.start_anchor = start_anchor
        self.end_anchor = end_anchor
        self.date_anchor = date_anchor
        self.engine = engine
        self.parsed_df = None
        if column_idx is None:
            self.column_idx = {
                "exchange_product_id": 1,
                "exchange_product_name": 2,
                "delivery_basis_name": 3,
                "volume": 4,
                "total": 5,
                "count": 14,
            }
        else:
            self.column_idx = column_idx

    def create_df(self, file: str) -> pd.DataFrame:
        df = pd.read_excel(file, sheet_name=0, engine=self.engine)  # type: ignore

        date_cell = df.astype(str).stack()[lambda s: s.str.contains(self.date_anchor)].squeeze()
        trade_date = pd.to_datetime(
            date_cell.replace(self.date_anchor, "").strip(),
            dayfirst=True,
            errors="raise",
        )

        start_idx = df.isin([self.start_anchor]).any(axis=1).idxmax() + 3
        end_idx = (df.iloc[start_idx:].isin([self.end_anchor]).any(axis=1)).idxmax()

        df_table = df.iloc[start_idx:end_idx, list(self.column_idx.values())].reset_index(drop=True)
        df_table.columns = list(self.column_idx.keys())

        df_table = df_table[pd.to_numeric(df_table["count"], errors="coerce").notna()]  # type: ignore
        df_table = df_table.reset_index(drop=True)

        numeric_columns = ["volume", "total", "count"]
        for col in numeric_columns:
            df_table[col] = pd.to_numeric(df_table[col], errors="coerce").astype("Int64")  # type: ignore

        df_table["date"] = trade_date
        df_table["oil_id"] = df_table["exchange_product_id"].str[:4]
        df_table["delivery_basis_id"] = df_table["exchange_product_id"].str[4:7]
        df_table["delivery_type_id"] = df_table["exchange_product_id"].str[-1]

        return df_table

    def parse(self) -> None:
        try:
            if self.files is None or len(self.files) == 0:
                raise ValueError("[Parser] Файлы для парсинга отсутствуют.")
            logger.info(f"[Parser] Получено {len(self.files)} файлов.")
        except ValueError as e:
            logger.info(e)
            return
        except Exception as e:
            logger.info(f"[Parser] Ошибка при проверке файлов: {e}")
            return

        logger.info(f"[Parser] Получено {len(self.files)} файлов.")
        df_list = [self.create_df(f) for f in self.files]
        combined_df = pd.concat(df_list, ignore_index=True)
        logger.info(f"[Parser] Отпарсено {len(combined_df)} строк.")

        self.parsed_df = combined_df
