from typing import Any

from pydantic import BaseModel, Field, field_validator


class ColumnDef(BaseModel):
    required: bool
    type: str | None = None

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str | None) -> str | None:
        if v is None:
            return v

        allowed = {
            "string",
            "int",
            "decimal",
            "float",
            "date",
            "datetime",
            "boolean",
        }

        if v not in allowed:
            raise ValueError(f"Unsupported column type: {v}")

        return v


class RuleDef(BaseModel):
    name: str
    columns: list[str] | None = None
    params: dict[str, Any] | None = None

class SheetDef(BaseModel):
    name: str
    required: bool
    header_row: int | None = None
    columns: dict[str, ColumnDef] | None = None
    rules: list[RuleDef] | None = None
    expectation_suite: list[str] | None = None

    def validate(self) -> None:
        if self.header_row is not None and self.header_row < 1:
            raise ValueError(
                f"Sheet '{self.name}': header_row must be >= 1"
            )

        if not self.columns:
            raise ValueError(
                f"Sheet '{self.name}': no columns defined"
            )

        # Validate rules reference valid columns
        column_names = set(self.columns.keys())

        for rule in self.rules or []:
            if rule.columns:
                missing = [c for c in rule.columns if c not in column_names]
                if missing:
                    raise ValueError(
                        f"Sheet '{self.name}': rule '{rule.name}' "
                        f"references unknown columns: {missing}"
                    )


class TemplateDef(BaseModel):
    template_id: str
    version: int = Field(..., ge=1)
    file_type: str
    file_pattern: str
    sheets: list[SheetDef]

    def validate(self) -> None:
        if not self.sheets:
            raise ValueError(
                f"Template '{self.template_id}': no sheets defined"
            )

        sheet_names = set()
        for sheet in self.sheets:
            if sheet.name in sheet_names:
                raise ValueError(
                    f"Template '{self.template_id}': duplicate sheet '{sheet.name}'"
                )
            sheet_names.add(sheet.name)
            sheet.validate()
