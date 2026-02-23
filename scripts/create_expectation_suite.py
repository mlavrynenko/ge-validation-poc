import logging
import argparse
import great_expectations as ge

from pathlib import Path
from core.logging_config import setup_logging
from file_parser.csv import CsvParser
from file_parser.excel import ExcelParser
from file_parser.parquet import ParquetParser
from data_loader.s3_loader import download_file_bytes
from template_engine.registry import TemplateRegistry
from template_engine.resolver import TemplateResolver

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = PROJECT_ROOT / "templates"
GX_DIR = PROJECT_ROOT / "gx"

PARSERS = {
    "csv": CsvParser,
    "excel": ExcelParser,
    "parquet": ParquetParser,
}


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Create GE expectation suite from template")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--template-id", required=True)
    parser.add_argument("--sheet-name", required=True)
    parser.add_argument("--suite-name", required=True)
    args = parser.parse_args()

    context = ge.get_context(context_root_dir=str(GX_DIR))

    registry = TemplateRegistry(str(TEMPLATES_DIR))
    resolver = TemplateResolver(registry.templates)

    template = resolver.resolve(args.dataset)
    if not template:
        raise ValueError(f"No template matched dataset: {args.dataset}")

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

    file_bytes = download_file_bytes(args.dataset)

    # ----------------------
    # Read dataset for suite creation
    # ----------------------

    parser = PARSERS[template.file_type]

    if template.file_type == "parquet":
        df = parser.read(
            file_bytes=file_bytes,
            usecols=list(sheet.columns.keys()) if sheet.columns else None,
        )

    elif template.file_type == "csv":
        df = parser.read(
            file_bytes=file_bytes,
            header=sheet.header_row,
            usecols=list(sheet.columns.keys()) if sheet.columns else None,
        )

    elif template.file_type == "excel":
        df = parser.read(
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

    for column, col_def in (sheet.columns or {}).items():
        validator.expect_column_to_exist(column)
        if col_def.required:
            validator.expect_column_values_to_not_be_null(column)

    validator.expect_table_row_count_to_be_between(min_value=1)
    context.save_expectation_suite(validator._expectation_suite)

    logger.info("Expectation suite '%s' created successfully", args.suite_name)


if __name__ == "__main__":
    main()
