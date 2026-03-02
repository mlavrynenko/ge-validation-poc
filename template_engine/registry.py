



class TemplateRegistry:
    def __init__(self, templates_path: str):
        self.templates_path = templates_path
        self.templates = self._load_templates()

    def validate(self) -> None:
        """
        Validate all loaded templates structurally.
        Raises ValueError on first invalid template.
        """
        if not self.templates:
            raise ValueError("No templates loaded")

        for template in self.templates:
            try:
                template.validate()
            except Exception as exc:
                raise ValueError(
                    f"Template '{template.template_id}' is invalid"
                ) from exc
