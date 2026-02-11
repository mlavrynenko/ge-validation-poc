import boto3
import pandas as pd
from io import BytesIO
from pathlib import Path

s3 = boto3.client("s3")


def parse_s3_path(path: str):
    parts = path.replace("s3://", "").split("/", 1)
    return parts[0], parts[1]


def detect_file_type(key: str) -> str:
    suffix = Path(key).suffix.lower()
    if suffix == ".csv":
        return "csv"
    elif suffix in [".xlsx", ".xls"]:
        return "excel"
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def load_dataframe_from_s3(s3_path: str) -> pd.DataFrame:
    bucket, key = parse_s3_path(s3_path)
    obj = s3.get_object(Bucket=bucket, Key=key)
    file_type = detect_file_type(key)

    if file_type == "csv":
        return pd.read_csv(obj["Body"])

    elif file_type == "excel":
        file_bytes = obj["Body"].read()          # ← read stream
        return pd.read_excel(BytesIO(file_bytes), engine="openpyxl")

    else:
        raise ValueError("Unsupported file type")
