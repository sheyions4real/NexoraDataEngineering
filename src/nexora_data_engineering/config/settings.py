"""
MODULE: Application Configuration

ARCHITECTURAL LAYER:
Configuration / Cross-cutting Infrastructure

PURPOSE:
Centralizes all application configuration required by the
Nexora Data Engineering platform.

RESPONSIBILITIES:
- Load application settings from environment variables and .env.
- Configure the source SQL Server connection.
- Configure Azure Data Lake Storage Gen2 connection details.
- Define environment-specific settings.
- Protect sensitive values such as database passwords using SecretStr.
- Provide a cached Settings instance to the application.

DATA ENGINEERING ROLE:
Provides the connection and environment configuration required
by ingestion pipelines to communicate with source databases and
destination data lake storage.

DESIGN CONSIDERATIONS:
- Credentials must not be hardcoded in application source code.
- Production secrets should eventually be supplied through
  Azure Key Vault or an equivalent secrets-management service.
- Business logic and extraction logic must not live in this module.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central application configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = "NexoraDataEngineering"
    app_environment: Literal["development", "testing", "production"] = "development"
    log_level: str = "INFO"

    # SQL Server
    db_server: str = "localhost"
    db_database: str = "NexoraEnterprise"
    db_username: str
    db_password: SecretStr
    db_driver: str = "ODBC Driver 18 for SQL Server"
    db_encrypt: bool = True
    db_trust_server_certificate: bool = True

    # Azure Data Lake
    azure_storage_account_url: str = Field(default="https://stnexoradatalake.dfs.core.windows.net")
    azure_storage_filesystem: str = "nexora"

    # Source database schemas
    source_schemas: str = "Sales"

    @property
    def configured_source_schemas(self) -> list[str]:
        """Return configured source schemas as a normalized list."""
        return [schema.strip() for schema in self.source_schemas.split(",") if schema.strip()]

    # Extraction
    extraction_batch_size: int = 10_000

    # Data lake layout
    bronze_root_path: str = "bronze"


@lru_cache
def get_settings() -> Settings:
    """Return a cached application settings instance."""
    return Settings()
