import pandas as pd


def csv_to_parquet(
    csv_path: str,
    parquet_path: str,
    delimiter: str = ",",
):
    df = pd.read_csv(
        csv_path,
        sep=delimiter,
    )

    df.to_parquet(
        parquet_path,
        engine="pyarrow",
        index=False,
    )


if __name__ == "__main__":
    csv_to_parquet(
        csv_path=r"C:\Users\mlavrynenko\Documents\N-iX\1. ValidationFramework\data_validation_framework\test_data\test_dataset_2.csv",
        parquet_path="csv_input.parquet",
    )
