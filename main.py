import sys
import logging
import argparse

from core.logging_config import setup_logging
from validation_engine.handler import handle_file


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Data Quality Validation Entry Point"
    )

    parser.add_argument(
        "--dataset",
        required=True,
        help="S3 path to input dataset (e.g. s3://bucket/key)",
    )

    parser.add_argument(
        "--expectations",
        required=True,
        help="Great Expectations expectation suite name",
    )

    parser.add_argument(
        "--results-bucket",
        required=True,
        help="S3 bucket for validation outputs",
    )

    args = parser.parse_args()

    logger.info(
        "Starting validation | dataset=%s | suite=%s",
        args.dataset,
        args.expectations,
    )

    try:
        handle_file(
            dataset=args.dataset,
            expectation_suite=args.expectations,
            results_bucket=args.results_bucket,
        )
    except Exception:
        logger.exception("Validation pipeline failed")
        sys.exit(1)

    logger.info("Validation pipeline completed successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()
