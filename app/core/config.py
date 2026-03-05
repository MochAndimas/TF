"""Centralized application configuration and environment profile loading."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Literal

from decouple import config as env
from pydantic import BaseModel

logger = logging.getLogger(__name__)

EnvironmentName = Literal["development", "production"]


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
    ACCESS_TOKEN_EXPIRE_MINUTE: int
    REFRESH_TOKEN_EXPIRE_DAYS: int
    ALGORITHM: str = "HS256"
    BOOTSTRAP_SUPERADMIN: bool = False
    INITIAL_SUPERADMIN_NAME: str | None = None
    INITIAL_SUPERADMIN_EMAIL: str | None = None
    INITIAL_SUPERADMIN_PASSWORD: str | None = None

    @property
    def cookie_secure(self) -> bool:
        """Whether secure cookies should be enforced.

        Returns:
            bool: ``True`` in production, ``False`` during development.
        """
        return not self.DEBUG

    @property
    def cors_origins(self) -> list[str]:
        """Resolve allowed CORS origins based on active environment.

        Returns:
            list[str]: Wildcard in debug mode, explicit frontend origin otherwise.
        """
        if self.DEBUG:
            return ["*"]
        return [self.FRONTEND_URL] if self.FRONTEND_URL else []

    def validate_bootstrap_config(self) -> None:
        """Ensure required bootstrap settings exist when bootstrap is enabled."""
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
    """Settings profile for local development workflow."""

    ENV: EnvironmentName = "development"
    DEBUG: bool = True
    DB_URL: str = env("DEV_DB_URL", cast=str)
    HOST: str = env("DEV_HOST", cast=str)
    PORT: int = env("DEV_PORT", cast=int)
    FRONTEND_URL: str | None = env("FRONTEND_URL", default=None, cast=str)
    WORKERS: int = env("WORKERS", default=1, cast=int)
    CSRF_SECRET: str = env("CSRF_SECRET", cast=str)
    JWT_SECRET_KEY: str = env("JWT_SECRET_KEY", cast=str)
    JWT_REFRESH_SECRET_KEY: str = env("JWT_REFRESH_SECRET_KEY", cast=str)
    ACCESS_TOKEN_EXPIRE_MINUTE: int = env("ACCESS_TOKEN_EXPIRE_MINUTES", cast=int)
    REFRESH_TOKEN_EXPIRE_DAYS: int = env("REFRESH_TOKEN_EXPIRE_DAYS", cast=int)
    BOOTSTRAP_SUPERADMIN: bool = env("BOOTSTRAP_SUPERADMIN", default=False, cast=bool)
    INITIAL_SUPERADMIN_NAME: str | None = env("INITIAL_SUPERADMIN_NAME", default=None, cast=str)
    INITIAL_SUPERADMIN_EMAIL: str | None = env("INITIAL_SUPERADMIN_EMAIL", default=None, cast=str)
    INITIAL_SUPERADMIN_PASSWORD: str | None = env("INITIAL_SUPERADMIN_PASSWORD", default=None, cast=str)


class ProductionSettings(Settings):
    """Settings profile for production deployment workflow."""

    ENV: EnvironmentName = "production"
    DEBUG: bool = False
    DB_URL: str = env("DB_URL", cast=str)
    HOST: str = env("HOST", cast=str)
    PORT: int = env("PORT", cast=int)
    FRONTEND_URL: str | None = env("FRONTEND_URL", default=None, cast=str)
    WORKERS: int = env("WORKERS", default=5, cast=int)
    CSRF_SECRET: str = env("CSRF_SECRET", cast=str)
    JWT_SECRET_KEY: str = env("JWT_SECRET_KEY", cast=str)
    JWT_REFRESH_SECRET_KEY: str = env("JWT_REFRESH_SECRET_KEY", cast=str)
    ACCESS_TOKEN_EXPIRE_MINUTE: int = env("ACCESS_TOKEN_EXPIRE_MINUTES", cast=int)
    REFRESH_TOKEN_EXPIRE_DAYS: int = env("REFRESH_TOKEN_EXPIRE_DAYS", cast=int)
    BOOTSTRAP_SUPERADMIN: bool = env("BOOTSTRAP_SUPERADMIN", default=False, cast=bool)
    INITIAL_SUPERADMIN_NAME: str | None = env("INITIAL_SUPERADMIN_NAME", default=None, cast=str)
    INITIAL_SUPERADMIN_EMAIL: str | None = env("INITIAL_SUPERADMIN_EMAIL", default=None, cast=str)
    INITIAL_SUPERADMIN_PASSWORD: str | None = env("INITIAL_SUPERADMIN_PASSWORD", default=None, cast=str)


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
