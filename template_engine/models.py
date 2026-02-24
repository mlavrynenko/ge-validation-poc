from pydantic import BaseModel
from typing import Dict, List, Optional, Any

class ColumnDef(BaseModel):
    required: bool
    type: Optional[str]

class RuleDef(BaseModel):
    name: str
    columns: Optional[List[str]] = None
    params: Optional[Dict[str, Any]] = None

class SheetDef(BaseModel):
    name: str
    required: bool
    header_row: Optional[int] = None
    columns: Optional[Dict[str, ColumnDef]] = None
    rules: Optional[List[RuleDef]] = None
    expectation_suite: Optional[List[str]] = None

class TemplateDef(BaseModel):
    template_id: str
    version: int
    file_type: str
    file_pattern: str
    sheets: List[SheetDef]
