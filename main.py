import argparse
import uuid
import sys
import json
import boto3
import pandas as pd
from pathlib import Path
from validation_engine.validation import validation_dataframe, save_run_to_db

s3 = boto3.client("s3")

#--dataset s3://dataquality-poc-input/incoming/<dataset>/input_dataset.csv
def parse_s3_path(path:str):
     parts = path.replace("s3://","").split("/",1)
     return parts[0], parts[1]

def extract_filename(key:str, run_id:str):
    #incoming/input_dataset.csv
    output_name = f"{run_id}_{Path(key).name}"
    return output_name

def save_outputs(input_bucket:str,
                 input_key:str,
                 output_bucket:str,
                 validation_result:dict,
                 run_id:str,
                 ):
    filename = extract_filename(input_key, run_id)

    validation_json = json.loads(json.dumps(validation_result, default=str))

    # 1️⃣ Save validation result JSON
    s3.put_object(
        Bucket=output_bucket,
        Key=f"validation-results/{filename}.validation.json",
        Body=json.dumps(validation_json, indent=2),
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--expectations", required=True)
    parser.add_argument("--results-bucket", required=True)
    args = parser.parse_args()

    print(f"Reading dataset from: {args.dataset}")
    print(f"Using expectation suite: {args.expectations}")

    #Load CSV from S3
    bucket, key = parse_s3_path(args.dataset)
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj["Body"])

    #Run GE validation
    result = validation_dataframe(df, args.expectations)

    run_id = str(uuid.uuid4())

    result.setdefault("meta", {}).update({
        "run_id": run_id,
        "input_bucket": bucket,
        "input_key": key,
        "validated_at": pd.Timestamp.utcnow().isoformat(),
        "expectation_suite": args.expectations,
        "row_count": len(df),
    })

    result["meta"].update(result["metrics"])

    print("💾 Saving validation run to Postgres...")
    save_run_to_db(result)
    print("✅ Saved to Postgres")

    #Save GE result
    save_outputs(
        input_bucket=bucket,
        input_key=key,
        output_bucket=args.results_bucket,
        validation_result=result,
        run_id=run_id,
    )

    print("\n🔎 Validation results per rule:\n")

    for idx, r in enumerate(result["results"], start=1):
        expectation = r["expectation_config"]["expectation_type"]
        kwargs = r["expectation_config"].get("kwargs", {})
        success = r["success"]

        status = "✅ PASS" if success else "❌ FAIL"

        print(f"{idx}. {expectation}")
        print(f"   Params : {kwargs}")
        print(f"   Result : {status}")

        if not success:
            print(f"   Details: {r.get('result')}")
        print()

    if not result["success"]:
        print("❌ Dataset validation FAILED")
        sys.exit(1)

    print("🎉 Dataset validation PASSED")
    sys.exit(0)


if __name__ == "__main__":
    main()
