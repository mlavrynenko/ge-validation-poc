import logging
import argparse
import great_expectations as ge

from core.logging_config import setup_logging
from file_parser.csv import CsvParser
from file_parser.excel import ExcelParser
from data_loader.s3_loader import download_file_bytes
from template_engine.registry import TemplateRegistry
from template_engine.resolver import TemplateResolver


PARSERS = {
    "excel": ExcelParser,
    "csv": CsvParser,
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

    context = ge.get_context(context_root_dir="../gx")

    registry = TemplateRegistry("../templates")
    resolver = TemplateResolver(registry.templates)

    template = resolver.resolve(args.dataset)
    if not template or template.template_id != args.template_id:
        raise ValueError("Template not found or template_id mismatch")

    sheet = next((s for s in template.sheets if s.name == args.sheet_name), None)
    if not sheet:
        raise ValueError(f"Sheet '{args.sheet_name}' not found in template")

    if template.file_type not in PARSERS:
        raise ValueError(f"Unsupported file type: {template.file_type}")

    file_bytes = download_file_bytes(args.dataset)

    df = PARSERS[template.file_type].read(
        file_bytes=file_bytes,
        sheet_name=sheet.name,
        header=sheet.header_row,
        usecols=list(sheet.columns.keys()) if sheet.columns else None,
    )

    validator = context.get_validator(
        batch_data=df,
        expectation_suite_name=args.suite_name,
        create_expectation_suite=True,
    )

    for column, col_def in (sheet.columns or {}).items():
        validator.expect_column_to_exist(column)
        if col_def.required:
            validator.expect_column_values_to_not_be_null(column)

    validator.expect_table_row_count_to_be_greater_than(0)
    validator.save_expectation_suite()

    logger.info("Expectation suite '%s' created successfully", args.suite_name)


if __name__ == "__main__":
    main()
