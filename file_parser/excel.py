import pandas as pd
from io import BytesIO

from file_parser.base import BaseParser

class ExcelParser(BaseParser):

    @staticmethod
    def read(
            file_bytes: bytes,
            sheet_name: str,
            header: int,
            usecols: list[str] | None = None,
    ) -> pd.DataFrame:
        buffer = BytesIO(file_bytes)
        return pd.read_excel(
            buffer,
            sheet_name = sheet_name,
            header = header - 1,
            usecols = usecols,
            engine = "openpyxl",
        )
