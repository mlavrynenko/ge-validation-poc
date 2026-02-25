from io import BytesIO

import pandas as pd

from file_parser.base import BaseParser


class ParquetParser(BaseParser):

    @staticmethod
    def read(
        file_bytes: bytes,
        usecols: list[str] | None = None,
    ) -> pd.DataFrame:
        buffer = BytesIO(file_bytes)

        return pd.read_parquet(
            buffer,
            columns=usecols,
            engine="pyarrow",
        )
