"""Application configuration loaded from environment variables."""
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Collector configuration.

    All values may be overridden via environment variables, e.g.
    METADATA_DB_URL, COLLECTOR_LOG_LEVEL, etc.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    metadata_db_url: str = Field(
        default="postgresql://metadata:metadata@localhost:5432/metadata",
        description="Postgres connection URL (async via asyncpg)",
    )
    db_pool_min_size: int = Field(default=2)
    db_pool_max_size: int = Field(default=10)

    # API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8080)
    api_prefix: str = Field(default="/api/v1")

    # Auth (placeholder; production should use Keycloak JWT)
    auth_required: bool = Field(default=False)
    auth_service_token: str = Field(default="dev-token-change-me")

    # Logging
    log_level: str = Field(default="INFO")

    # Limits
    max_lineage_depth: int = Field(default=10)
    default_lineage_depth: int = Field(default=3)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
