import pandas as pd
from pathlib import Path

def excel_to_parquet(
    excel_path: str,
    parquet_path: str,
    sheet_name: str | int = 0,
):
    df = pd.read_excel(
        excel_path,
        sheet_name=sheet_name,
        engine="openpyxl",
    )

    df.to_parquet(
        parquet_path,
        engine="pyarrow",
        index=False,
    )


if __name__ == "__main__":
    excel_to_parquet(
        excel_path=r"C:\Users\mlavrynenko\Documents\N-iX\1. ValidationFramework\data_validation_framework\test_data\Excel Template for testing.xlsx",
        parquet_path="input.parquet",
        sheet_name="Tab A",
    )
