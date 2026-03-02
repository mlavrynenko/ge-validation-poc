from pathlib import Path

import yaml

from template_engine.models import TemplateDef


class TemplateRegistry:
    def __init__(self, templates_path: str):
        self.templates_path = Path(templates_path)
        self.templates = self._load_templates()

    def _load_templates(self) -> list[TemplateDef]:
        if not self.templates_path.exists():
            raise ValueError(
                f"Templates path does not exist: {self.templates_path}"
            )

        templates: list[TemplateDef] = []

        for path in sorted(self.templates_path.rglob("*.yml")) + sorted(
            self.templates_path.rglob("*.yaml")
        ):
            with path.open("r", encoding="utf-8") as f:
                try:
                    raw = yaml.safe_load(f)
                except yaml.YAMLError as exc:
                    raise ValueError(
                        f"Invalid YAML in template file: {path}"
                    ) from exc

            if not raw:
                raise ValueError(f"Template file is empty: {path}")

            try:
                template = TemplateDef(**raw)
            except Exception as exc:
                raise ValueError(
                    f"Failed to parse template {path}"
                ) from exc

            templates.append(template)

        if not templates:
            raise ValueError(
                f"No templates found in {self.templates_path}"
            )

        return templates

    def validate(self) -> None:
        """
        Validate all loaded templates structurally.
        Raises ValueError on first invalid template.
        """
        for template in self.templates:
            try:
                template.validate()
            except Exception as exc:
                raise ValueError(
                    f"Template '{template.template_id}' is invalid"
                ) from exc
