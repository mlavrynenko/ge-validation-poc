import yaml
from pathlib import Path

from template_engine.models import TemplateDef

class TemplateRegistry:
    def __init__(self, templates_path: str):
        self.templates = self._load_templates(templates_path)

    def _load_templates(self, path: str):
        templates = []
        for file in Path(path).glob("*.yaml"):
            with open(file) as f:
                raw = yaml.safe_load(f)
                templates.append(TemplateDef(**raw))
        return templates
