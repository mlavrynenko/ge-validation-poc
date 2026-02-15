import argparse
import great_expectations as ge

from data_loader.s3_loader import load_dataframe_from_s3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--suite-name", required=True)
    args = parser.parse_args()

    context = ge.get_context(context_root_dir="gx")

    # Load sample data
    df = load_dataframe_from_s3(args.dataset)

    # Create validator
    validator = context.get_validator(
        batch_data=df,
        expectation_suite_name=args.suite_name,
        create_expectation_suite=True,
    )

    # Add expectations
    validator.expect_column_to_exist("id")
    validator.expect_column_values_to_not_be_null("id")
    validator.expect_table_row_count_to_be_between(min_value=5)

    # Save expectation suite
    validator.save_expectation_suite()

    print(f"Expectation suite '{args.suite_name}' created successfully")


if __name__ == "__main__":
    main()
