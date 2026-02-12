from psycopg2.extras import execute_batch


def insert_rule_results(result: dict, cur) -> None:
    """
    Insert rule-level validation results into validation_rule_results table.
    """

    records = []

    for r in result["results"]:
        records.append(
            (
                result["meta"]["run_id"],
                result["meta"]["validated_at"],
                result["meta"]["input_key"],
                r["expectation_config"]["expectation_type"],
                r["expectation_config"].get("kwargs", {}).get("column"),
                r["success"],
                r.get("result", {}).get("unexpected_count", 0),
            )
        )

    execute_batch(
        cur,
        """
        INSERT INTO validation_rule_results (
            run_id,
            validated_at,
            dataset,
            expectation_type,
            column_name,
            success,
            unexpected_count
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        records
    )
