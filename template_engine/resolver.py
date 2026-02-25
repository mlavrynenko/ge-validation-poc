import re
from typing import Optional
from template_engine.models import TemplateDef

class TemplateResolver:

    def __init__(self, templates: list[TemplateDef]):
        self.templates = templates

    def resolve(self, dataset: str) -> Optional[TemplateDef]:
        matches: list[TemplateDef] = []

        for template in self.templates:
            if re.search(template.file_pattern, dataset):
                matches.append(template)

        if not matches:
            return None

        # Guardrail: multiple template_ids matching the same dataset
        template_ids = {t.template_id for t in matches}
        if len(template_ids) > 1:
            raise ValueError(
                f"Multiple template_ids match dataset '{dataset}': "
                f"{sorted(template_ids)}"
            )

        # Pick the highest version
        selected = max(matches, key=lambda t: t.version)

        return selected
