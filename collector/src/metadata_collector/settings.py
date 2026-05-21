"""Application configuration.

Loading priority (highest → lowest):
  1. Environment variables  (e.g. METADATA_DB_URL=...)
  2. config/config.yaml     (path overridable via METADATA_CONFIG_FILE env var)
  3. Hardcoded defaults

The YAML file keys must match Settings field names exactly (flat structure).
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Tuple, Type

from pydantic import Field
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


class Settings(BaseSettings):
    """Collector configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    metadata_db_url: str = Field(
        default="postgresql://metadata:metadata@localhost:5432/metadata",
    )
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
