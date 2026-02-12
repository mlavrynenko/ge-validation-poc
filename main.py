import sys
import uuid
import json
import boto3
import logging
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

from db.connection import get_db_cursor
from core.logging_config import setup_logging

from repository.validation_run_repository import insert_validation_run
from repository.validation_rule_repository import insert_rule_results

from data_loader.s3_loader import load_dataframe_from_s3, parse_s3_path
from validation_engine.validation import validation_dataframe

s3 = boto3.client("s3")

def generate_run_timestamp():
    return datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")

def extract_filename(key:str, timestamp:str):
    #incoming/input_dataset.csv
    output_name = f"{timestamp}_{Path(key).name}"
    return output_name

def save_outputs(input_bucket:str,
                 input_key:str,
                 output_bucket:str,
                 validation_result:dict,
                 run_id:str,
                 ):
    filename = extract_filename(input_key, run_id)

    validation_json = json.dumps(validation_result, default=str, indent=2)

    # 1️⃣ Save validation result JSON
    s3.put_object(
        Bucket=output_bucket,
        Key=f"validation-results/{filename}.validation.json",
        Body=validation_json,
        ContentType="application/json",
    )

    # 2️⃣ Route CSV based on validation result
    status_prefix = "passes" if validation_result["success"] else "failed"

    s3.copy_object(
        Bucket=output_bucket,
        CopySource={"Bucket": input_bucket, "Key":input_key},
        Key=f"{status_prefix}/{filename}",
    )


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--expectations", required=True)
    parser.add_argument("--results-bucket", required=True)
    args = parser.parse_args()

    logger.info("Reading dataset from: %s", args.dataset)
    logger.info("Using expectation suite: %s", args.expectations)

    #Load CSV from S3
    bucket, key = parse_s3_path(args.dataset)
    df = load_dataframe_from_s3(args.dataset)

    #Run GE validation
    result = validation_dataframe(df, args.expectations)

    run_id = str(uuid.uuid4())
    run_timestamp = generate_run_timestamp()

    result.setdefault("meta", {}).update({
        "run_id": run_id,
        "run_timestamp": run_timestamp,
        "input_bucket": bucket,
        "input_key": key,
        "validated_at": pd.Timestamp.utcnow().isoformat(),
        "expectation_suite": args.expectations,
        "row_count": len(df),
    })

    result["meta"].update(result["metrics"])

    logger.info("Saving validation run to Postgres...")

    try:
        with get_db_cursor() as cur:
            insert_validation_run(result, cur)
            insert_rule_results(result, cur)

        logger.info("Saved to Postgres")

    except Exception as e:
        logger.exception("DB transaction failed")
        raise

    #Save GE result
    save_outputs(
        input_bucket=bucket,
        input_key=key,
        output_bucket=args.results_bucket,
        validation_result=result,
        run_id=run_timestamp,
    )

    logger.info("Validation report saved to S3 bucket: %s", args.results_bucket)

    logger.info("Validation results per rule:")

    for idx, r in enumerate(result["results"], start=1):
        expectation = r["expectation_config"]["expectation_type"]
        column = r["expectation_config"].get("kwargs", {}).get("column")
        unexpected = r.get("result", {}).get("unexpected_count", 0)
        success = r["success"]

        logger.info(
            "Rule %s | %s | column=%s | %s | unexpected=%s",
            idx,
            expectation,
            column,
            "PASS" if success else "FAIL",
            unexpected
        )

        if not success:
            logger.warning("Details: %s", r.get("result"))

    if not result["success"]:
        logger.error("Dataset validation FAILED")
        sys.exit(1)

    logger.info("Dataset validation PASSED")
    sys.exit(0)


if __name__ == "__main__":
    main()
