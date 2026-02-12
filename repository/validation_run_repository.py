def insert_validation_run(result: dict, cur) -> None:
    """
    Insert a validation run summary into validation_runs table.
    """

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
