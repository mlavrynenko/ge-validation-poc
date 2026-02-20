from psycopg2.extras import Json

def insert_structural_result(
    result: dict,
    meta: dict,
    sheet_name: str,
    cur
) -> None:
    """
    Insert structural validation result for a single sheet
    """

    cur.execute(
        """
        INSERT INTO structural_validation_results (
            run_id,
            dataset,
            template_id,
            template_version,
            sheet_name,
            passed,
            error_count,
            warning_count,
            errors,
            warnings,
            validated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            meta["run_id"],
            meta["input_key"],
            meta["template_id"],
            meta["template_version"],
            sheet_name,
            result["passed"],
            len(result["errors"]),
            len(result["warnings"]),
            Json(result["errors"]),
            Json(result["warnings"]),
            meta["validated_at"],
        )
    )
