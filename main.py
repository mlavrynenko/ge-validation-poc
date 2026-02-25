import sys
import os
import logging
import argparse

from core.settings import load_settings
from core.logging_config import setup_logging
from validation_engine.handler import handle_file


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    settings = load_settings()

    logger.info("Starting validation | env=%s", settings.APP_ENV)

    parser = argparse.ArgumentParser(
        description="Data Quality Validation Entry Point"
    )

    parser.add_argument(
        "--dataset",
        required=True,
        help="S3 path to input dataset (e.g. s3://bucket/key)",
    )

    parser.add_argument(
        "--results-bucket",
        required=False,
        help="S3 bucket for validation outputs",
    )

    args = parser.parse_args()

    # 🔑 CLI override wins
    if args.results_bucket:
        os.environ["RESULTS_BUCKET"] = args.results_bucket
        logger.info(
            "RESULTS_BUCKET set from CLI | bucket=%s",
            args.results_bucket,
        )

    logger.info(
        "Starting validation | dataset=%s",
        args.dataset,
    )

    try:
        result = handle_file(args.dataset)
    except Exception:
        logger.exception("Validation pipeline failed")
        sys.exit(1)

    logger.info(
        "Validation completed | run_id=%s success=%s",
        result["run_id"],
        result["success"],
    )

    if result["outputs_enabled"]:
        logger.info(
            "Validation artefacts stored at %s",
            result["results_location"],
        )
    else:
        logger.info("S3 artefacts not written (RESULTS_BUCKET not set)")

    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
