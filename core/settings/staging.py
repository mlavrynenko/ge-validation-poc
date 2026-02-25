from .base import BaseSettings, SettingsError


class StagingSettings(BaseSettings):
    """
    Staging configuration.
    """

    def __init__(self) -> None:
        super().__init__()

        if not self.RESULTS_BUCKET:
            raise SettingsError(
                "RESULTS_BUCKET must be set in staging environment"
            )

        self.DEBUG = False
        