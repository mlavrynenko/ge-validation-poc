from .base import BaseSettings


class DevSettings(BaseSettings):
    """
    Development configuration.
    """

    def __init__(self, skip_secrets: bool = False) -> None:
        super().__init__(skip_secrets=skip_secrets)
        self.DEBUG = True
        