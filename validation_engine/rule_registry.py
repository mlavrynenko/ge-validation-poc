from collections.abc import Callable
from typing import Any

import pandas as pd

from template_engine.models import RuleDef, SheetDef

RuleFn = Callable[[RuleDef, Any, SheetDef], None]

def _get_mostly(rule: RuleDef) -> float | None:
    if not rule.params:
        return None

    mostly = rule.params.get("mostly")

    if mostly is None:
        return None

    if not isinstance(mostly, (int, float)):
        raise ValueError("Parameter 'mostly' must be a number")

    if not (0 < mostly <= 1):
        raise ValueError("Parameter 'mostly' must be in (0, 1]")

    return float(mostly)

def _require_columns(
    sheet: SheetDef,
    columns: list[str] | None,
    rule_name: str,
) -> list[str]:
    if not columns:
        raise ValueError(
            f"Rule '{rule_name}' requires explicit columns, "
            "but none were provided"
        )

    if not sheet.columns:
        raise ValueError(
            f"Rule '{rule_name}' requires columns, "
            "but no columns are defined in the template"
        )

    missing = [c for c in columns if c not in sheet.columns]
    if missing:
        raise ValueError(
            f"Rule '{rule_name}' requires columns {missing}, "
            f"but they are not defined in the template"
        )

    return columns


# -------------------------
# Core rules
# -------------------------

def rule_not_null_required(rule: RuleDef, validator, sheet: SheetDef) -> None:
    if not sheet.columns:
        raise ValueError(
            "Rule 'not_null_required' requires columns in the template"
        )

    for col, col_def in sheet.columns.items():
        if col_def.required:
            validator.expect_column_values_to_not_be_null(
                col,
                result_format="SUMMARY",
            )


def rule_positive(rule: RuleDef, validator, sheet: SheetDef) -> None:
    columns = _require_columns(sheet, rule.columns, "positive")
    mostly = _get_mostly(rule)

    for col in columns:
        validator.expect_column_values_to_be_between(
            col,
            min_value=0,
            max_value=None,
            strict_min=True,
            mostly=mostly,
            result_format="SUMMARY",
        )


def rule_unique(rule: RuleDef, validator, sheet: SheetDef) -> None:
    columns = _require_columns(sheet, rule.columns, "unique")
    mostly = _get_mostly(rule)

    for col in columns:
        validator.expect_column_values_to_be_unique(
            col,
            mostly=mostly,
            result_format="SUMMARY",
        )

def rule_distinct_values_in_set(rule: RuleDef, validator, sheet: SheetDef) -> None:
    """
    Expect column values to be in an allowed set.
    Supports `mostly`.
    """
    columns = _require_columns(sheet, rule.columns, "distinct_values_in_set")

    if len(columns) != 1:
        raise ValueError(
            "Rule 'distinct_values_in_set' supports exactly one column"
        )

    if not rule.params or "allowed_values" not in rule.params:
        raise ValueError(
            "Rule 'distinct_values_in_set' requires params.allowed_values"
        )

    allowed_values = rule.params["allowed_values"]

    if not isinstance(allowed_values, list) or not allowed_values:
        raise ValueError(
            "Rule 'distinct_values_in_set' requires a non-empty list of allowed_values"
        )

    if mostly is not None:
        if not isinstance(mostly, (int, float)) or not (0 < mostly <= 1):
            raise ValueError(
                "Rule 'distinct_values_in_set' param 'mostly' must be a float in (0, 1]"
            )

    mostly = _get_mostly(rule)
    column = columns[0]

    kwargs = {
        "column": column,
        "value_set": allowed_values,
        "result_format": "SUMMARY",
    }

    # Only pass mostly if explicitly defined
    if mostly is not None:
        kwargs["mostly"] = mostly

    validator.expect_column_values_to_be_in_set(**kwargs)


# -------------------------
# Date rules
# -------------------------

def rule_date_format(rule: RuleDef, validator, sheet: SheetDef) -> None:
    """
    Validate string-formatted dates (CSV / Excel only).
    Safely skips already-typed datetime columns.
    """
    columns = _require_columns(sheet, rule.columns, "date_format")
    fmt = rule.params.get("format", "%Y-%m-%d") if rule.params else "%Y-%m-%d"

    for col in columns:
        series = validator.get_column(col)

        if pd.api.types.is_datetime64_any_dtype(series):
            continue

        validator.expect_column_values_to_match_strftime_format(
            col,
            fmt,
            result_format="SUMMARY",
        )


def rule_date_type(rule: RuleDef, validator, sheet: SheetDef) -> None:
    columns = _require_columns(sheet, rule.columns, "date_type")

    for col in columns:
        series = validator.get_column(col)

        # Iceberg DATE → pandas object[datetime.date]
        if series.dtype == "object":
            # This is a valid Iceberg DATE → do NOT type-check
            continue

        validator.expect_column_values_to_be_of_type(
            col,
            type_="datetime64",
            result_format="SUMMARY",
        )

# -------------------------
# Rule registry
# -------------------------

RULES: dict[str, RuleFn] = {
    "not_null_required": rule_not_null_required,
    "positive": rule_positive,
    "unique": rule_unique,
    "date_format": rule_date_format,
    "date_type": rule_date_type,
    "distinct_values_in_set": rule_distinct_values_in_set,
}


def apply_rule(rule: RuleDef, validator, sheet: SheetDef) -> None:
    fn = RULES.get(rule.name)

    if not fn:
        raise ValueError(f"Unknown rule: {rule.name}")

    fn(rule, validator, sheet)
