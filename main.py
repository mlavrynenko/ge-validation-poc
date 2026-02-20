import sys
import logging
import argparse

from core.logging_config import setup_logging
from validation_engine.handler import handle_file


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Data Quality Validation Entry Point")
    parser.add_argument(
        "--dataset",
        required=True,
        help="S3 path to input dataset (e.g. s3://bucket/key)",
    )
    args = parser.parse_args()

    logger.info("Starting validation for dataset: %s", args.dataset)

    try:
        handle_file(args.dataset)
    except Exception:
        logger.exception("Validation pipeline failed")
        sys.exit(1)

    logger.info("Validation pipeline completed successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()
