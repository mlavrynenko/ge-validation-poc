import os
from .dev import DevSettings
from .staging import StagingSettings
from .prod import ProdSettings
from .base import SettingsError

def load_settings():
    env = os.getenv("APP_ENV")

    if env == "ci":
        return DevSettings(skip_secrets=True)

    if not env:
        raise SettingsError("APP_ENV is not set")

    match env:
        case "dev":
            return DevSettings()
        case "staging":
            return StagingSettings()
        case "prod":
            return ProdSettings()
        case _:
            raise SettingsError(f"Unknown APP_ENV '{env}'")
