from typing import Callable, Any
from template_engine.models import SheetDef, RuleDef

RuleFn = Callable[[RuleDef, Any, SheetDef], None]


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


def rule_not_null_required(rule: RuleDef, validator, sheet: SheetDef) -> None:
    if not sheet.columns:
        return

    for col, col_def in sheet.columns.items():
        if col_def.required:
            validator.expect_column_values_to_not_be_null(col)


def rule_positive(rule: RuleDef, validator, sheet: SheetDef) -> None:
    columns = _require_columns(sheet, rule.columns, "positive")

    for col in columns:
        validator.expect_column_values_to_be_between(
            col,
            min_value=0,
            strictly=True,
        )


def rule_unique(rule: RuleDef, validator, sheet: SheetDef) -> None:
    columns = _require_columns(sheet, rule.columns, "unique")

    for col in columns:
        validator.expect_column_values_to_be_unique(col)


def rule_date_format(rule: RuleDef, validator, sheet: SheetDef) -> None:
    columns = _require_columns(sheet, rule.columns, "date_format")

    fmt = rule.params.get("format", "%Y-%m-%d") if rule.params else "%Y-%m-%d"

    for col in columns:
        validator.expect_column_values_to_match_strftime_format(col, fmt)


# ---- rule registry ----

RULES: dict[str, RuleFn] = {
    "not_null_required": rule_not_null_required,
    "positive": rule_positive,
    "unique": rule_unique,
    "date_format": rule_date_format,
}


def apply_rule(rule: RuleDef, validator, sheet: SheetDef) -> None:
    fn = RULES.get(rule.name)

    if not fn:
        raise ValueError(f"Unknown rule: {rule.name}")

    fn(rule, validator, sheet)
