from .base import BaseSettings


class DevSettings(BaseSettings):
    """
    Development configuration.
    """

    def __init__(self) -> None:
        super().__init__()

        # Dev allows missing RESULTS_BUCKET
        self.DEBUG = True
        