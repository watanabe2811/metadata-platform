"""Application configuration.

Loading priority (highest → lowest):
  1. Environment variables  (e.g. METADATA_DB_URL=...)
  2. config/config.yaml     (path overridable via METADATA_CONFIG_FILE env var)
  3. Hardcoded defaults

The YAML file keys must match Settings field names exactly (flat structure).

DB credentials can be set in two ways (a) or (b):

  (a) Full URL — metadata_db_url: postgresql://user:pass@host:5432/dbname
  (b) Split fields — metadata_db_url without credentials + db_user + db_password
      e.g. metadata_db_url: postgresql://host:5432/dbname
           db_user: metadata
           db_password: metadata

When db_user / db_password are provided, they are injected into metadata_db_url
even if the URL already contains a userinfo component (they take precedence).
Env vars DB_USER / DB_PASSWORD map to the split fields.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Tuple, Type
from urllib.parse import urlparse, urlunparse

from pydantic import Field, model_validator
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        import yaml  # type: ignore[import-untyped]
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


class _YamlConfigSource(PydanticBaseSettingsSource):
    """Reads flat key=value pairs from a YAML file."""

    def __init__(self, settings_cls: Type[BaseSettings], yaml_path: Path) -> None:
        super().__init__(settings_cls)
        self._data = _load_yaml(yaml_path)

    def get_field_value(self, field: FieldInfo, field_name: str) -> Tuple[Any, str, bool]:
        return self._data.get(field_name), field_name, False

    def __call__(self) -> dict[str, Any]:
        return {k: v for k, v in self._data.items() if v is not None}


def _default_config_path() -> Path:
    override = os.getenv("METADATA_CONFIG_FILE")
    if override:
        return Path(override)
    return Path("config/config.yaml")


def _inject_credentials(url: str, user: str | None, password: str | None) -> str:
    """Return url with user:password injected into the netloc."""
    if not user:
        return url
    parsed = urlparse(url)
    host_part = parsed.hostname or ""
    if parsed.port:
        host_part = f"{host_part}:{parsed.port}"
    userinfo = f"{user}:{password}" if password else user
    new_netloc = f"{userinfo}@{host_part}"
    return urlunparse(parsed._replace(netloc=new_netloc))


class Settings(BaseSettings):
    """Collector configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database — full URL (credentials optional if db_user/db_password are set)
    metadata_db_url: str = Field(
        default="postgresql://localhost:5432/metadata",
    )
    # Split credential fields; mapped from env vars DB_USER / DB_PASSWORD
    db_user: str | None = Field(default=None)
    db_password: str | None = Field(default=None)

    db_pool_min_size: int = Field(default=2)
    db_pool_max_size: int = Field(default=10)

    # API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8080)
    api_prefix: str = Field(default="/api/v1")

    # Auth
    auth_required: bool = Field(default=False)
    auth_service_token: str = Field(default="dev-token-change-me")

    # Logging
    log_level: str = Field(default="INFO")

    # Limits
    max_lineage_depth: int = Field(default=10)
    default_lineage_depth: int = Field(default=3)

    @model_validator(mode="after")
    def _inject_db_credentials(self) -> Settings:
        if self.db_user:
            self.metadata_db_url = _inject_credentials(
                self.metadata_db_url, self.db_user, self.db_password
            )
        return self

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            _YamlConfigSource(settings_cls, _default_config_path()),
            dotenv_settings,
            file_secret_settings,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
