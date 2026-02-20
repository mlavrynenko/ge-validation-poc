import uuid
import logging
from datetime import datetime

from core.logging_config import setup_logging

from file_parser.csv import CsvParser
from file_parser.excel import ExcelParser
from db.connection import get_db_cursor
from data_loader.s3_loader import download_file_bytes
from template_engine.registry import TemplateRegistry
from template_engine.resolver import TemplateResolver
from validation_engine.validation import validate_dataframe
from validation_engine.structural import StructuralValidationError, run_structural_checks
from repository.validation_rule_repository import insert_rule_results
from repository.validation_run_repository import insert_validation_run
from repository.structural_validation_repository import insert_structural_result

setup_logging()
logger = logging.getLogger(__name__)

PARSERS = {
    "excel": ExcelParser,
    "csv": CsvParser,
}

registry = TemplateRegistry("templates")
resolver = TemplateResolver(registry.templates)


def handle_file(s3_path: str):
    template = resolver.resolve(s3_path)
    if not template:
        raise ValueError("No template matches file")

    if template.file_type not in PARSERS:
        raise ValueError(f"Unsupported file type: {template.file_type}")

    file_bytes = download_file_bytes(s3_path)

    run_id = uuid.uuid4()
    validated_at = datetime.utcnow()

    meta = {
        "run_id": run_id,
        "input_key": s3_path,
        "validated_at": validated_at,
        "template_id": template.template_id,
        "template_version": template.version,
    }

    parser = PARSERS[template.file_type]

    with get_db_cursor() as cur:
        for sheet in template.sheets:
            logger.info("Processing sheet '%s'", sheet.name)

            df = parser.read(
                file_bytes=file_bytes,
                sheet_name=sheet.name,
                header=sheet.header_row,
                usecols=list(sheet.columns.keys()) if sheet.columns else None,
            )

            # 1️⃣ Structural validation
            try:
                structural_result = run_structural_checks(df, sheet)
            except StructuralValidationError as e:
                structural_result = e.args[0]

                insert_structural_result(
                    result=structural_result,
                    meta=meta,
                    sheet_name=sheet.name,
                    cur=cur,
                )

                logger.error(
                    "Structural validation failed for sheet '%s': %s",
                    sheet.name,
                    structural_result["errors"],
                )
                return

            insert_structural_result(
                result=structural_result,
                meta=meta,
                sheet_name=sheet.name,
                cur=cur,
            )

            # 2️⃣ GE validation
            for suite_name in sheet.expectations or []:
                ge_result = validate_dataframe(df, suite_name)

                ge_result["meta"] = {
                    **meta,
                    "row_count": len(df),
                    **ge_result["metrics"],
                }

                insert_validation_run(ge_result, cur)
                insert_rule_results(ge_result, cur)

                logger.info(
                    "GE validation completed for sheet '%s' with suite '%s'",
                    sheet.name,
                    suite_name,
                )
