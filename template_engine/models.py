from pydantic import BaseModel
from typing import Dict, List, Optional

class ColumnDef(BaseModel):
    required: bool
    type: Optional[str]

class SheetDef(BaseModel):
    name: str
    required: bool
    header_row: int
    columns: Optional[Dict[str, ColumnDef]] = None
    expectations: Optional[List[str]] = None

class TemplateDef(BaseModel):
    template_id: str
    version: int
    file_type: str
    file_pattern: str
    sheets: List[SheetDef]
