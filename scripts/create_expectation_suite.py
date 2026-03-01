import os
import logging
import argparse
from pathlib import Path

import great_expectations as ge

from core.logging_config import setup_logging
from data_loader.s3_loader import download_file_bytes
from file_parser.csv import CsvParser
from file_parser.excel import ExcelParser
from file_parser.iceberg import IcebergParser
from file_parser.parquet import ParquetParser
from template_engine.registry import TemplateRegistry
from template_engine.resolver import TemplateResolver
from validation_engine.rule_registry import apply_rule

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = PROJECT_ROOT / "templates"
GX_DIR = PROJECT_ROOT / "gx"

PARSERS = {
    "csv": CsvParser,
    "excel": ExcelParser,
    "parquet": ParquetParser,
    "iceberg": IcebergParser,
}


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    arg_parser = argparse.ArgumentParser(
        description="Create GE expectation suite from template"
    )
    arg_parser.add_argument("--dataset", required=True)
    arg_parser.add_argument("--template-id", required=True)
    arg_parser.add_argument("--sheet-name", required=True)
    arg_parser.add_argument("--suite-name", required=True)
    args = arg_parser.parse_args()

    context = ge.get_context(context_root_dir=str(GX_DIR))

    registry = TemplateRegistry(str(TEMPLATES_DIR))
    resolver = TemplateResolver(registry.templates)

    template = resolver.resolve(args.dataset)
    if not template:
        raise ValueError(f"No template matched dataset: {args.dataset}")

    logger.info(
        "Using template %s v%s with columns=%s",
        template.template_id,
        template.version,
        list(template.sheets[0].columns.keys())
    )

    if template.template_id != args.template_id:
        raise ValueError(
            f"Template ID mismatch: expected '{args.template_id}', "
            f"got '{template.template_id}'"
        )

    sheet = next((s for s in template.sheets if s.name == args.sheet_name), None)
    if not sheet:
        raise ValueError(f"Sheet '{args.sheet_name}' not found in template")

    if template.file_type not in PARSERS:
        raise ValueError(f"Unsupported file type: {template.file_type}")

    # ----------------------
    # Read dataset for suite creation
    # ----------------------

    data_parser = PARSERS[template.file_type]

    if template.file_type == "iceberg":
        df = data_parser.read(
            table_identifier=args.dataset.replace("iceberg://", ""),
            columns=list(sheet.columns.keys()) if sheet.columns else None,
        )

    else:
        file_bytes = download_file_bytes(args.dataset)

        if template.file_type == "parquet":
            df = data_parser.read(
                file_bytes=file_bytes,
                usecols=list(sheet.columns.keys()) if sheet.columns else None,
            )

        elif template.file_type == "csv":
            df = data_parser.read(
                file_bytes=file_bytes,
                header=sheet.header_row,
                usecols=list(sheet.columns.keys()) if sheet.columns else None,
            )

        elif template.file_type == "excel":
            df = data_parser.read(
                file_bytes=file_bytes,
                sheet_name=sheet.name,
                header=sheet.header_row,
                usecols=list(sheet.columns.keys()) if sheet.columns else None,
            )

        else:
            raise ValueError(f"Unsupported file type for suite creation: {template.file_type}")

    validator = ge.from_pandas(df)
    validator.context = context
    validator._expectation_suite.expectation_suite_name = args.suite_name

    if template.file_type == "iceberg":
        removed = [
            e for e in validator._expectation_suite.expectations
            if e.expectation_type == "expect_column_values_to_be_of_type"
        ]

        if removed:
            logger.info(
                "Removed %d expect_column_values_to_be_of_type expectations "
                "for Iceberg dataset",
                len(removed),
            )

        validator._expectation_suite.expectations = [
            e for e in validator._expectation_suite.expectations
            if e.expectation_type != "expect_column_values_to_be_of_type"
        ]

    # Always ensure expected columns exist
    for column in (sheet.columns or {}).keys():
        validator.expect_column_to_exist(column)

    if not sheet.rules:
        raise ValueError(
            f"No rules defined for sheet '{sheet.name}' "
            f"in template '{template.template_id}'"
        )

    for rule in sheet.rules:
        apply_rule(rule, validator, sheet)

    validator.expect_table_row_count_to_be_between(min_value=1)
    context.save_expectation_suite(validator._expectation_suite)

    # DEV-ONLY: build Data Docs
    if os.getenv("APP_ENV") == "dev":
        context.build_data_docs()
        logger.info("GE Data Docs generated (local dev only)")

    logger.info("Expectation suite '%s' created successfully", args.suite_name)


if __name__ == "__main__":
    main()
