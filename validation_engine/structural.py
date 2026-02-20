import pandas as pd
from typing import Dict, Any

from template_engine.models import SheetDef

class StructuralValidationError(Exception):
    """
    Raised when structural vaildation fails.
    """
    pass

def run_structural_checks(
        df: pd.DataFrame,
        sheet_def: SheetDef,
) -> Dict[str, Any]:
    """
    Run structural (schema-level) validation on a DataFrame
    based on the SheetDef template.

    Returns a dict with structural validation results.
    Raises StructuralValidationError on hard failures.
    """
    results = {
        "sheet_name": sheet_def.name,
        "passed": True,
        "errors": [],
        "warnings": [],
    }

    #Empty dataframe check
    if df.empty:
        results["passed"] = False
        results["errors"].append("Sheet is empty")
        raise StructuralValidationError(results)

    #Column presence check
    if sheet_def.columns:
        expected_columns = set(sheet_def.columns.keys())
        actual_columns = set(df.columns)

        #Reauired columns
        required_columns = {
            name for name, col in sheet_def.columns.items()
            if col.required
        }

        missing_columns = required_columns - actual_columns
        if missing_columns:
            results["passed"] = False
            results["errors"].append(
                f"Missing required columns: {sorted(missing_columns)}"
            )

        #Unexpected columns (warning only)
        unexpected_columns = actual_columns - expected_columns
        if unexpected_columns:
            results["warnings"].append(
                f"Unexpected columns present: {sorted(unexpected_columns)}"
            )

    # Final decision
    if not results["passed"]:
        raise StructuralValidationError(results)

    return results
