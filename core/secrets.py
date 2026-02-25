import json
import boto3
from core.errors import SettingsError

class SecretsManagerError(RuntimeError):
    pass

class SecretsManager:
    def __init__(self, region: str | None = None):
        self.client = boto3.client(
            "secretsmanager",
            region_name=region,
        )

    def get_secret(self, secret_id: str) -> dict:
        try:
            response = self.client.get_secret_value(
                SecretId=secret_id
            )
        except Exception as exc:
            raise SettingsError(
                f"Failed to retrieve secret '{secret_id}'"
            ) from exc

        secret_string = response.get("SecretString")
        if not secret_string:
            raise SecretsManagerError(
                f"Secret '{secret_id}' has no SecretString"
            )

        return json.loads(secret_string)
