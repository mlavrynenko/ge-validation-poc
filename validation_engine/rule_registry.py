from typing import Callable
from template_engine.models import SheetDef


def _require_columns(sheet: SheetDef, columns: list[str], rule: str) -> None:
    missing = [c for c in columns if c not in sheet.columns]
    if missing:
        raise ValueError(
            f"Rule '{rule}' requires columns {missing}, "
            f"but they are not defined in the template"
        )


def apply_rule(rule_name: str, validator, sheet: SheetDef) -> None:
    """
    Apply a named validation rule to a Great Expectations validator
    using sheet-level metadata.
    """

    rules: dict[str, Callable[[], None]] = {

        # -------------------------
        # Required columns not null
        # -------------------------
        "not_null_required": lambda: [
            validator.expect_column_values_to_not_be_null(col)
            for col, col_def in sheet.columns.items()
            if col_def.required
        ],

        # -------------------------
        # Positive numeric amounts
        # -------------------------
        "positive_amounts": lambda: (
            _require_columns(sheet, ["order_amount"], "positive_amounts"),
            validator.expect_column_values_to_be_between(
                "order_amount",
                min_value=0,
                strictly=True,
            ),
        ),

        # -------------------------
        # Unique identifier
        # -------------------------
        "unique_id": lambda: (
            _require_columns(sheet, ["id"], "unique_id"),
            validator.expect_column_values_to_be_unique("id"),
        ),

        # -------------------------
        # Date format validation
        # -------------------------
        "valid_dates": lambda: (
            _require_columns(sheet, ["created_at"], "valid_dates"),
            validator.expect_column_values_to_match_strftime_format(
                "created_at",
                "%Y-%m-%d",
            ),
        ),
    }

    rule = rules.get(rule_name)
    if not rule:
        raise ValueError(f"Unknown expectation rule: '{rule_name}'")

    rule()
