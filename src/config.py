"""Application settings loaded from environment and secrets.

Uses pydantic-settings for validation and a JSON secrets file for sensitive values (e.g. DSN).
"""

import os

from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings

from app_secrets.json_secrets import JsonSecretManager as SecretManager

SECRETS_FILE = os.path.join(os.path.dirname(__file__), "../secrets.json")
secrets_manager = SecretManager(SECRETS_FILE)


class Settings(BaseSettings):
    """Runtime settings for the message bus (app name, Postgres DSN)."""

    app_name: str = Field(default="PostgreSQL Message Bus")
    pgmq_dsn: PostgresDsn = Field(secrets_manager.get_secret("PGMQ_DSN"))


def get_settings() -> Settings:
    """Return the loaded settings instance."""
    return Settings()
