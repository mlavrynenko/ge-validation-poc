from .base import BaseSettings, SettingsError


class ProdSettings(BaseSettings):
    """
    Production configuration.
    """

    def __init__(self) -> None:
        super().__init__()

        if not self.RESULTS_BUCKET:
            raise SettingsError(
                "RESULTS_BUCKET must be set in production environment"
            )

        self.DEBUG = False
