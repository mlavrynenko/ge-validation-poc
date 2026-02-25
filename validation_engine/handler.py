import uuid
import logging
import os
import re
from datetime import datetime

from core.logging_config import setup_logging

from file_parser.csv import CsvParser
from file_parser.excel import ExcelParser
from file_parser.parquet import ParquetParser
from file_parser.iceberg import IcebergParser

from db.connection import get_db_cursor
from data_loader.s3_loader import download_file_bytes
from data_loader.s3_writer import upload_json, upload_bytes

from template_engine.registry import TemplateRegistry
from template_engine.resolver import TemplateResolver

from validation_engine.validation import validate_dataframe
from validation_engine.structural import (
    StructuralValidationError,
    run_structural_checks,
)

from repository.validation_rule_repository import insert_rule_results
from repository.validation_run_repository import insert_validation_run
from repository.structural_validation_repository import insert_structural_result


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
setup_logging()
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Parsers
# -------------------------------------------------------------------
PARSERS = {
    "csv": CsvParser,
    "excel": ExcelParser,
    "parquet": ParquetParser,
    "iceberg": IcebergParser,
}


registry = TemplateRegistry("templates")
resolver = TemplateResolver(registry.templates)


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def safe_name(value: str) -> str:
    """
    Convert string to S3 / filesystem-safe name.
    """
    value = value.strip().replace(" ", "_")
    value = re.sub(r"[^a-zA-Z0-9_.-]", "", value)
    return value


def build_key(
    *,
    validated_at: datetime,
    dataset: str,
    sheet: str,
    extension: str,
    prefix: str,
) -> str:
    """
    <prefix>/<timestamp>__<dataset>__<sheet>.<ext>
    """
    timestamp = validated_at.strftime("%Y-%m-%dT%H-%M-%S")
    return (
        f"{prefix}/"
        f"{timestamp}__{safe_name(dataset)}__{safe_name(sheet)}.{extension}"
    )


def init_run_summary(meta: dict) -> dict:
    return {
        "meta": {
            "run_id": meta["run_id"],
            "input_key": meta["input_key"],
            "validated_at": meta["validated_at"],
            "row_count": 0,
            "validation_duration_ms": 0,
            "rules_total": 0,
            "rules_passed": 0,
            "rules_failed": 0,
            "quality_score": 1.0,
            "null_ratio": 0.0,
            "duplicate_ratio": 0.0,
            "schema_changed": False,
            "invalid_row_count": 0,
        },
        "success": True,
    }


def accumulate_metrics(run_summary: dict, ge_result: dict) -> None:
    metrics = ge_result["metrics"]
    meta = run_summary["meta"]

    meta["row_count"] = max(meta["row_count"], ge_result["meta"]["row_count"])
    meta["validation_duration_ms"] += metrics["validation_duration_ms"]
    meta["rules_total"] += metrics["rules_total"]
    meta["rules_passed"] += metrics["rules_passed"]
    meta["rules_failed"] += metrics["rules_failed"]
    meta["invalid_row_count"] += metrics["invalid_row_count"]

    meta["null_ratio"] = max(meta["null_ratio"], metrics["null_ratio"])
    meta["duplicate_ratio"] = max(meta["duplicate_ratio"], metrics["duplicate_ratio"])
    meta["schema_changed"] |= metrics["schema_changed"]

    if metrics["rules_failed"] > 0:
        run_summary["success"] = False

    if meta["rules_total"] > 0:
        meta["quality_score"] = round(
            meta["rules_passed"] / meta["rules_total"], 4
        )


# -------------------------------------------------------------------
# Main handler
# -------------------------------------------------------------------
def handle_file(s3_path: str) -> dict:
    """
    Validate dataset and optionally persist results to S3.
    """

    # ---- runtime config (NOT import-time) ----
    RESULTS_BUCKET = os.getenv("RESULTS_BUCKET")
    ENABLE_S3_OUTPUTS = bool(RESULTS_BUCKET)

    if ENABLE_S3_OUTPUTS:
        logger.info("S3 outputs enabled | bucket=%s", RESULTS_BUCKET)
    else:
        logger.info("S3 outputs disabled (RESULTS_BUCKET not set)")

    # ---- template resolution ----
    template = resolver.resolve(s3_path)
    if not template:
        raise ValueError("No template matches file")

    logger.info(
        "Using template %s v%s for dataset=%s",
        template.template_id,
        template.version,
        s3_path,
    )

    if template.file_type not in PARSERS:
        raise ValueError(f"Unsupported file type: {template.file_type}")

    parser = PARSERS[template.file_type]

    # ---- read input ----
    file_bytes = (
        None if template.file_type == "iceberg"
        else download_file_bytes(s3_path)
    )

    run_id = str(uuid.uuid4())
    validated_at = datetime.utcnow()
    dataset_name = s3_path.split("/")[-1]

    meta = {
        "run_id": run_id,
        "input_key": s3_path,
        "validated_at": validated_at,
        "template_id": template.template_id,
        "template_version": template.version,
    }

    run_summary = init_run_summary(meta)

    # ---- main execution ----
    with get_db_cursor() as cur:
        for sheet in template.sheets:
            logger.info("Processing sheet '%s'", sheet.name)

            # Read data
            if template.file_type == "iceberg":
                df_raw = parser.read(
                    table_identifier=s3_path.replace("iceberg://", ""),
                    columns=None,
                )
            else:
                read_kwargs = {"file_bytes": file_bytes}
                if template.file_type == "excel":
                    read_kwargs.update(
                        {
                            "sheet_name": sheet.name,
                            "header": sheet.header_row,
                        }
                    )
                df_raw = parser.read(**read_kwargs)

            # Structural validation
            try:
                structural_result = run_structural_checks(df_raw, sheet)
            except StructuralValidationError as e:
                structural_result = e.args[0]

                insert_structural_result(
                    result=structural_result,
                    meta=meta,
                    sheet_name=sheet.name,
                    cur=cur,
                )

                run_summary["success"] = False
                logger.error(
                    "Structural validation failed | sheet=%s errors=%s",
                    sheet.name,
                    structural_result["errors"],
                )
                continue

            insert_structural_result(
                result=structural_result,
                meta=meta,
                sheet_name=sheet.name,
                cur=cur,
            )

            # GE validation
            df = (
                df_raw.loc[:, list(sheet.columns.keys())]
                if sheet.columns
                else df_raw
            )

            if not sheet.expectation_suite:
                raise ValueError(
                    f"No validation expectation_suite defined for sheet '{sheet.name}' "
                    f"in template '{template.template_id}'"
                )

            if sheet.rules:
                logger.warning(
                    "Rules are defined in template but ignored at runtime | sheet=%s",
                    sheet.name,
                )

            for suite_name in sheet.expectation_suite:
                ge_result = validate_dataframe(df, suite_name)

                ge_result["meta"] = {
                    **meta,
                    "sheet_name": sheet.name,
                    "row_count": len(df),
                    **ge_result["metrics"],
                }

                # ---- S3: GE JSON ----
                if ENABLE_S3_OUTPUTS:
                    ge_key = build_key(
                        validated_at=validated_at,
                        dataset=dataset_name,
                        sheet=sheet.name,
                        extension="json",
                        prefix="validation-results",
                    )

                    logger.info(
                        "Uploading GE result | bucket=%s key=%s",
                        RESULTS_BUCKET,
                        ge_key,
                    )

                    upload_json(
                        bucket=RESULTS_BUCKET,
                        key=ge_key,
                        payload=ge_result,
                    )

                accumulate_metrics(run_summary, ge_result)
                insert_rule_results(ge_result, cur)

                logger.info(
                    "GE validation completed | sheet=%s suite=%s",
                    sheet.name,
                    suite_name,
                )

        # Persist run summary (single row)
        insert_validation_run(run_summary, cur)

    # ---- S3: archive original dataset ----
    if ENABLE_S3_OUTPUTS and file_bytes:
        status_prefix = "passes" if run_summary["success"] else "failed"

        archive_key = build_key(
            validated_at=validated_at,
            dataset=dataset_name,
            sheet="dataset",
            extension=dataset_name.split(".")[-1],
            prefix=status_prefix,
        )

        upload_bytes(
            bucket=RESULTS_BUCKET,
            key=archive_key,
            content=file_bytes,
        )

    return {
        "run_id": run_id,
        "success": run_summary["success"],
        "outputs_enabled": ENABLE_S3_OUTPUTS,
        "results_location": (
            f"s3://{RESULTS_BUCKET}/validation-results/"
            if ENABLE_S3_OUTPUTS
            else None
        ),
    }
