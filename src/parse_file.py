from typing import Literal

import pandas as pd


class SpimexParser:
    def __init__(
        self,
        table_name: str = "Единица измерения: Метрическая тонна",
        end_marker: str = "Итого:",
        date_marker: str = "Дата торгов:",
        column_idx: dict[str, int] | None = None,
        engine: Literal["xlrd", "openpyxl", "odf", "pyxlsb", "calamine"] = "xlrd",
    ):
        self.table_name = table_name
        self.end_marker = end_marker
        self.date_marker = date_marker
        self.engine = engine
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

    def parse(self, file: str) -> pd.DataFrame:
        df = pd.read_excel(file, sheet_name=0, engine=self.engine)  # type: ignore

        date_cell = df.astype(str).stack()[lambda s: s.str.contains(self.date_marker)].squeeze()  # type: ignore
        trade_date = pd.to_datetime(date_cell.replace(self.date_marker, "").strip(), dayfirst=True, errors="raise")  # type: ignore

        start_idx = df.isin([self.table_name]).any(axis=1).idxmax() + 3  # type: ignore
        end_idx = (df.iloc[start_idx:].isin([self.end_marker]).any(axis=1)).idxmax()  # type: ignore

        df_table = df.iloc[start_idx:end_idx, list(self.column_idx.values())].reset_index(drop=True)  # type: ignore
        df_table.columns = list(self.column_idx.keys())

        df_table = df_table[pd.to_numeric(df_table["count"], errors="coerce").notna()]  # type: ignore
        df_table = df_table.reset_index(drop=True)  # type: ignore

        df_table["date"] = trade_date
        df_table["oil_id"] = df_table["exchange_product_id"].str[:4]  # type: ignore
        df_table["delivery_basis_id"] = df_table["exchange_product_id"].str[4:7]  # type: ignore
        df_table["delivery_type_id"] = df_table["exchange_product_id"].str[-1]  # type: ignore

        return df_table  # type: ignore


if __name__ == "__main__":
    parser = SpimexParser()
    table = parser.parse("bulletins/oil_xls_20230109162000.xls")
    print(table)
