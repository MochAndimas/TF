"""Centralized application configuration and environment profile loading."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse
from typing import Literal

from decouple import config as env
from pydantic import BaseModel

logger = logging.getLogger(__name__)

EnvironmentName = Literal["development", "production"]


def _read_setting(name: str, *, default=None, cast=str, secret: bool = False):
    """Read one configuration value with support for file-based secret indirection.

    The helper first checks ``<NAME>_FILE`` so deployments can inject secrets as
    mounted files instead of plain environment variables. When no file override
    exists, it falls back to the regular environment variable handled by
    ``python-decouple``.

    Args:
        name: Base configuration key name, for example ``JWT_SECRET_KEY``.
        default: Fallback value used when neither ``<NAME>`` nor
            ``<NAME>_FILE`` is present.
        cast: Conversion callable forwarded to decouple or applied to the file
            contents.
        secret: Indicates that the setting contains secret material, allowing
            the loader to emit lower-signal debug diagnostics only.

    Returns:
        Any: Casted configuration value loaded from either the secret file or
        the plain environment variable.
    """
    file_path = env(f"{name}_FILE", default=None, cast=str)
    if file_path and str(file_path).strip().lower() != "none":
        raw_value = Path(file_path).read_text(encoding="utf-8").strip()
        return cast(raw_value) if cast is not str else raw_value

    value = env(name, default=default, cast=cast)
    if secret and value not in (None, "") and os.getenv(f"{name}_FILE") is None:
        logger.debug("Secret %s loaded from direct environment variable", name)
    return value


class Settings(BaseModel):
    """Base application settings shared across all environments.

    Attributes:
        API_V1_STR (str): Root prefix for API version 1 routes.
        PROJECT_NAME (str): Human-readable project name for docs and metadata.
        ENV (EnvironmentName): Active environment name.
        DEBUG (bool): Flag to enable developer-friendly behavior.
        HOST (str): Host interface for API server binding.
        PORT (int): Port value for API server binding.
        DB_URL (str): SQLAlchemy async database URL.
        FRONTEND_URL (str | None): Allowed frontend origin for CORS in production.
        WORKERS (int): Number of Uvicorn workers for production-like runtime.
        CSRF_SECRET (str): Secret key used by session middleware.
        JWT_SECRET_KEY (str): Secret for access token signing.
        JWT_REFRESH_SECRET_KEY (str): Secret for refresh token signing.
        ACCESS_TOKEN_EXPIRE_MINUTE (int): Access-token lifetime in minutes.
        REFRESH_TOKEN_EXPIRE_DAYS (int): Refresh-token lifetime in days.
        ALGORITHM (str): JWT signing algorithm.
    """

    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Traders Family Campaign Data API V1.0"

    ENV: EnvironmentName
    DEBUG: bool
    HOST: str
    PORT: int
    DB_URL: str
    FRONTEND_URL: str | None
    WORKERS: int
    CSRF_SECRET: str

    JWT_SECRET_KEY: str
    JWT_REFRESH_SECRET_KEY: str
    APP_ENCRYPTION_KEY: str
    ACCESS_TOKEN_EXPIRE_MINUTE: int
    REFRESH_TOKEN_EXPIRE_DAYS: int
    ALGORITHM: str = "HS256"
    BOOTSTRAP_SUPERADMIN: bool = False
    INITIAL_SUPERADMIN_NAME: str | None = None
    INITIAL_SUPERADMIN_EMAIL: str | None = None
    INITIAL_SUPERADMIN_PASSWORD: str | None = None

    @staticmethod
    def _split_origins(raw_value: str | None) -> list[str]:
        """Parse a comma-separated origins string into a normalized list.

        Args:
            raw_value (str | None): Raw environment value containing zero or
                more origins separated by commas.

        Returns:
            list[str]: Trimmed non-empty origins with trailing slashes removed.
        """
        if not raw_value:
            return []
        return [item.strip().rstrip("/") for item in raw_value.split(",") if item.strip()]

    @property
    def cookie_secure(self) -> bool:
        """Whether secure cookies should be enforced.

        Returns:
            bool: ``True`` in production, ``False`` during development.
        """
        return not self.DEBUG

    @property
    def cookie_samesite(self) -> str:
        """Return the canonical SameSite policy for session-related cookies.

        Returns:
            str: SameSite policy string reused across auth and session cookies.
        """
        return "lax"

    @property
    def cors_origins(self) -> list[str]:
        """Resolve allowed CORS origins based on active environment.

        Returns:
            list[str]: Explicit allowed frontend origins for current environment.
        """
        default_dev_origins = [
            "http://localhost:5504",
            "http://127.0.0.1:5504",
            "http://localhost:8501",
            "http://127.0.0.1:8501",
        ]
        if self.DEBUG:
            configured_dev_origins = self._split_origins(self.FRONTEND_URL)
            return configured_dev_origins or default_dev_origins
        return self._split_origins(self.FRONTEND_URL)

    @property
    def trusted_hosts(self) -> list[str]:
        """Resolve the set of allowed hosts used for Host-header validation.

        Returns:
            list[str]: Sorted list of hostnames accepted by trusted-host
            middleware in the current environment.
        """
        hosts = {"localhost", "127.0.0.1"}
        if self.HOST not in {"0.0.0.0", "::"}:
            hosts.add(self.HOST)
        if self.FRONTEND_URL:
            parsed = urlparse(self.FRONTEND_URL.split(",")[0].strip())
            if parsed.hostname:
                hosts.add(parsed.hostname)
        return sorted(hosts)

    def validate_bootstrap_config(self) -> None:
        """Validate bootstrap-superadmin settings before bootstrap runs.

        Returns:
            None: Completes silently when configuration is sufficient.

        Raises:
            ValueError: When bootstrap mode is enabled but one or more required
            bootstrap settings are missing.
        """
        if not self.BOOTSTRAP_SUPERADMIN:
            return

        required_fields = {
            "INITIAL_SUPERADMIN_NAME": self.INITIAL_SUPERADMIN_NAME,
            "INITIAL_SUPERADMIN_EMAIL": self.INITIAL_SUPERADMIN_EMAIL,
            "INITIAL_SUPERADMIN_PASSWORD": self.INITIAL_SUPERADMIN_PASSWORD,
        }
        missing_fields = [key for key, value in required_fields.items() if not value]
        if missing_fields:
            raise ValueError(
                "Missing bootstrap env vars: " + ", ".join(missing_fields)
            )


class DevelopmentSettings(Settings):
    """Settings profile tuned for local development, debugging, and localhost."""

    ENV: EnvironmentName = "development"
    DEBUG: bool = True
    DB_URL: str = env("DEV_DB_URL", cast=str)
    HOST: str = env("DEV_HOST", cast=str)
    PORT: int = env("DEV_PORT", cast=int)
    FRONTEND_URL: str | None = env("FRONTEND_URL", default=None, cast=str)
    WORKERS: int = env("WORKERS", default=1, cast=int)
    CSRF_SECRET: str = _read_setting("CSRF_SECRET", cast=str, secret=True)
    JWT_SECRET_KEY: str = _read_setting("JWT_SECRET_KEY", cast=str, secret=True)
    JWT_REFRESH_SECRET_KEY: str = _read_setting("JWT_REFRESH_SECRET_KEY", cast=str, secret=True)
    APP_ENCRYPTION_KEY: str = _read_setting(
        "APP_ENCRYPTION_KEY",
        default="",
        cast=str,
        secret=True,
    )
    ACCESS_TOKEN_EXPIRE_MINUTE: int = _read_setting("ACCESS_TOKEN_EXPIRE_MINUTES", cast=int)
    REFRESH_TOKEN_EXPIRE_DAYS: int = _read_setting("REFRESH_TOKEN_EXPIRE_DAYS", cast=int)
    BOOTSTRAP_SUPERADMIN: bool = env("BOOTSTRAP_SUPERADMIN", default=False, cast=bool)
    INITIAL_SUPERADMIN_NAME: str | None = env("INITIAL_SUPERADMIN_NAME", default=None, cast=str)
    INITIAL_SUPERADMIN_EMAIL: str | None = env("INITIAL_SUPERADMIN_EMAIL", default=None, cast=str)
    INITIAL_SUPERADMIN_PASSWORD: str | None = _read_setting(
        "INITIAL_SUPERADMIN_PASSWORD",
        default=None,
        cast=str,
        secret=True,
    )


class ProductionSettings(Settings):
    """Settings profile tuned for production deployment and stricter defaults."""

    ENV: EnvironmentName = "production"
    DEBUG: bool = False
    DB_URL: str = env("DB_URL", cast=str)
    HOST: str = env("HOST", cast=str)
    PORT: int = env("PORT", cast=int)
    FRONTEND_URL: str | None = env("FRONTEND_URL", default=None, cast=str)
    WORKERS: int = env("WORKERS", default=5, cast=int)
    CSRF_SECRET: str = _read_setting("CSRF_SECRET", cast=str, secret=True)
    JWT_SECRET_KEY: str = _read_setting("JWT_SECRET_KEY", cast=str, secret=True)
    JWT_REFRESH_SECRET_KEY: str = _read_setting("JWT_REFRESH_SECRET_KEY", cast=str, secret=True)
    APP_ENCRYPTION_KEY: str = _read_setting(
        "APP_ENCRYPTION_KEY",
        default="",
        cast=str,
        secret=True,
    )
    ACCESS_TOKEN_EXPIRE_MINUTE: int = _read_setting("ACCESS_TOKEN_EXPIRE_MINUTES", cast=int)
    REFRESH_TOKEN_EXPIRE_DAYS: int = _read_setting("REFRESH_TOKEN_EXPIRE_DAYS", cast=int)
    BOOTSTRAP_SUPERADMIN: bool = env("BOOTSTRAP_SUPERADMIN", default=False, cast=bool)
    INITIAL_SUPERADMIN_NAME: str | None = env("INITIAL_SUPERADMIN_NAME", default=None, cast=str)
    INITIAL_SUPERADMIN_EMAIL: str | None = env("INITIAL_SUPERADMIN_EMAIL", default=None, cast=str)
    INITIAL_SUPERADMIN_PASSWORD: str | None = _read_setting(
        "INITIAL_SUPERADMIN_PASSWORD",
        default=None,
        cast=str,
        secret=True,
    )


@lru_cache
def get_settings() -> Settings:
    """Load and cache settings object based on ``ENV`` value.

    Returns:
        Settings: Environment-specific concrete settings instance.
    """
    env_name = env("ENV", default="development", cast=str).lower()
    if env_name == "production":
        logger.info("Loading production settings")
        return ProductionSettings()

    logger.info("Loading development settings")
    return DevelopmentSettings()


settings = get_settings()
