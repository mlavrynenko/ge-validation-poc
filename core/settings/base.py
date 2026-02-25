import logging
import os

from core.errors import SettingsError
from core.logging_config import setup_logging
from core.secrets import SecretsManager

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
setup_logging()
logger = logging.getLogger(__name__)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value or value.strip() == "":
        raise SettingsError(
            f"Required environment variable '{name}' is not set or empty"
        )
    return value


class BaseSettings:
    """
    Base configuration shared by all environments.
    """

    def __init__(self, skip_secrets: bool = False):
        # Environment
        self.APP_ENV = require_env("APP_ENV")

        if skip_secrets:
            logger.info("Running in CI mode: Secrets Manager disabled")
            self.DB_HOST = "localhost"
            self.DB_PORT = 5432
            self.DB_NAME = "dummy"
            self.DB_USER = "dummy"
            self.DB_PASSWORD = "dummy"
            return

        # Secrets Manager
        self.DB_SECRET_ID = require_env("DB_SECRET_ID")
        self.AWS_REGION = os.getenv("AWS_REGION")

        # Load DB credentials from Secrets Manager
        secrets = SecretsManager(region=self.AWS_REGION)
        db_secret = secrets.get_secret(self.DB_SECRET_ID)

        # Map secret fields → settings
        self.DB_HOST = db_secret["host"]
        self.DB_PORT = db_secret.get("port", 5432)
        self.DB_NAME = db_secret["dbname"]
        self.DB_USER = db_secret["username"]
        self.DB_PASSWORD = db_secret["password"]
