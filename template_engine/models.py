from typing import Any

from pydantic import BaseModel


class ColumnDef(BaseModel):
    required: bool
    type: str | None

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

class TemplateDef(BaseModel):
    template_id: str
    version: int
    file_type: str
    file_pattern: str
    sheets: list[SheetDef]
