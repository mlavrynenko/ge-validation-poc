import logging
import re

from template_engine.models import TemplateDef

logger = logging.getLogger(__name__)


class TemplateResolver:
    def __init__(self, templates: list[TemplateDef]):
        self.templates = templates

        logger.info(
            "Loaded %d templates: %s",
            len(templates),
            [(t.template_id, t.version, t.file_pattern) for t in templates],
        )

    def resolve(self, dataset: str) -> TemplateDef | None:
        logger.info("Resolving template for dataset='%s'", dataset)

        matches: list[TemplateDef] = []

        for template in self.templates:
            matched = bool(re.search(template.file_pattern, dataset))
            logger.info(
                "Testing template=%s v%s pattern=%r → %s",
                template.template_id,
                template.version,
                template.file_pattern,
                matched,
            )

            if matched:
                matches.append(template)

        if not matches:
            logger.error("No templates matched dataset '%s'", dataset)
            return None

        template_ids = {t.template_id for t in matches}
        if len(template_ids) > 1:
            raise ValueError(
                f"Multiple template_ids match dataset '{dataset}': "
                f"{sorted(template_ids)}"
            )

        selected = max(matches, key=lambda t: t.version)

        logger.info(
            "Selected template %s v%s",
            selected.template_id,
            selected.version,
        )

        return selected
