import os
import time
import psycopg2
import great_expectations as ge
import pandas as pd

def save_run_to_db(result):
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=5432,
            connect_timeout=5,
        )

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO validation_runs(
                run_id, 
                dataset, 
                success, 
                validated_at, 
                row_count,
                validation_duration_ms,
                rules_total,
                rules_passed,
                rules_failed,
                quality_score,
                null_ratio,
                duplicate_ratio,
                schema_changed,
                invalid_row_count                                    
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                result["meta"]["run_id"],
                result["meta"]["input_key"],
                result["success"],
                result["meta"]["validated_at"],
                result["meta"]["row_count"],
                result["meta"]["validation_duration_ms"],
                result["meta"]["rules_total"],
                result["meta"]["rules_passed"],
                result["meta"]["rules_failed"],
                result["meta"]["quality_score"],
                result["meta"]["null_ratio"],
                result["meta"]["duplicate_ratio"],
                result["meta"]["schema_changed"],
                result["meta"]["invalid_row_count"],
            )
        )
        conn.commit()

    except Exception as e:
        print(f"❌ DB WRITE FAILED: {e}")
        raise

    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()


def validation_dataframe(df: pd.DataFrame, suite_name: str) -> dict:
    start = time.time()

    validator = ge.from_pandas(df)

    context = ge.get_context()
    suite = context.get_expectation_suite(suite_name)

    validator._expectation_suite = suite

    ge_result = validator.validate(result_format="SUMMARY").to_json_dict()

    duration_ms = int((time.time() - start) * 1000)

    #Metrics
    rules_total = len(ge_result["results"])
    rules_passed = sum(r["success"] for r in ge_result["results"])
    rules_failed = rules_total - rules_passed
    quality_score = round(rules_passed / rules_total, 4) if rules_total else 1.0

    #Null ratio
    total_cells = df.size
    null_cells = df.isna().sum().sum()
    null_ratio = round(null_cells / total_cells, 4) if total_cells else 0

    #Duplicate ratio
    duplicate_ratio = round(df.duplicated().sum() / len(df), 4) if len(df) else 0

    #Invalid rows estimate (from falling expectations)
    invalid_raw_count = sum(
        r.get("result", {}).get("unexpected_count", 0)
        for r in ge_result["results"]
        if not r["success"]
    )

    #Schema drift detection
    expected_columns = [e["kwargs"]["column"] for e in suite.expectations if "column" in e["kwargs"]]
    schema_changed = set(df.columns) != set(expected_columns)

    ge_result["metrics"] = {
        "validation_duration_ms": duration_ms,
        "rules_total": rules_total,
        "rules_passed": rules_passed,
        "rules_failed": rules_failed,
        "quality_score": quality_score,
        "null_ratio": null_ratio,
        "duplicate_ratio": duplicate_ratio,
        "schema_changed": schema_changed,
        "invalid_row_count": invalid_raw_count,
    }

    return ge_result
