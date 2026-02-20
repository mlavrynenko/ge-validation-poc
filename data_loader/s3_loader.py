import boto3

s3 = boto3.client("s3")

def parse_s3_path(path: str):
    parts = path.replace("s3://", "").split("/", 1)
    return parts[0], parts[1]

def download_file_bytes(s3_path: str) -> bytes:
    bucket, key = parse_s3_path(s3_path)
    obj = s3.get_object(Bucket = bucket, Key = key)
    return obj["Body"].read()
