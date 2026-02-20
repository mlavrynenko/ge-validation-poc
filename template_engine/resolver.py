import re
from typing import Optional

from template_engine.models import TemplateDef

class TemplateResolver:

    def __init__(self, templates: list[TemplateDef]):
        self.templates = templates

    def resolve(self, filename: str) -> Optional[TemplateDef]:
        matches = []

        for template in self.templates:
            if re.match(template.file_pattern, filename):
                matches.append(template)
        if not matches:
            return None

        return sorted(
            matches,
            key = lambda t: t.version,
            reverse = True
        )[0]
