import great_expectations as ge
import pandas as pd


def main():
    context = ge.get_context(context_root_dir="gx")

    suite_name = "basic_data_quality_checks"

    # Load sample data
    df = pd.read_csv("test_data/valid_dataset.csv")

    # Create validator properly via DataContext
    validator = context.get_validator(
        batch_data=df,
        expectation_suite_name=suite_name
    )

    # Add expectations
    validator.expect_column_to_exist("id")
    validator.expect_column_values_to_not_be_null("id")
    validator.expect_table_row_count_to_be_between(min_value=5)

    # Save expectation suite
    validator.save_expectation_suite()

    print(f"Expectation suite '{suite_name}' created successfully")


if __name__ == "__main__":
    main()
