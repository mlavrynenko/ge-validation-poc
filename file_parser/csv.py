import pandas as pd
from io import BytesIO

from file_parser.base import BaseParser

class CsvParser(BaseParser):

    @staticmethod
    def read(file_bytes: bytes,
             header: int = 1,
             usecols: list[str] | None = None,
             delimiter: str = ","
             ) -> pd.DataFrame:

        buffer = BytesIO(file_bytes)
        return pd.read_csv(
            buffer,
            header=header - 1,
            usecols=usecols,
            sep=delimiter,
        )
