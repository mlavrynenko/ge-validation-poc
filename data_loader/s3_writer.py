import json
import boto3
from datetime import datetime

s3 = boto3.client("s3")

def upload_bytes(
    bucket: str,
    key: str,
    content: bytes,
    content_type: str = "application/octet-stream",
) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=content,
        ContentType=content_type,
    )

def upload_json(bucket: str, key: str, payload: dict) -> None:
    upload_bytes(
        bucket=bucket,
        key=key,
        content=json.dumps(payload, indent=2).encode("utf-8"),
        content_type="application/json",
    )
