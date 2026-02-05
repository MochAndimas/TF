from functools import lru_cache
from pydantic_settings import BaseSettings
from decouple import config


class Settings(BaseSettings):
    """
    Common settings for all environments
    """
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Traders Family Campaign Data API V1.0"
    
    # JWT TOKEN
    JWT_SECRET_KEY: str = config("JWT_SECRET_KEY", cast=str)
    JWT_REFRESH_SECRET_KEY: str = config("JWT_REFRESH_SECRET_KEY", cast=str)
    ACCESS_TOKEN_EXPIRE_MINUTE: int = config("ACCESS_TOKEN_EXPIRE_MINUTE", cast=int)
    REFRESH_TOKEN_EXPIRE_DAYS: int = config("REFRESH_TOKEN_EXPIRE_DAYS", cast=int)
    ALGORITHM: str = "HS256"


class ProductionSettings(Settings):
    """
    Production-specific settings
    """
    DEBUG: bool = False

    # SQLITE database
    DB_URL: str = "sqlite+aiosqlite:///./app/db/campaign_data.db"


class DevelopmentSettings(Settings):
    """
    Docstring for DevelopmentSettings
    """
    DEBUG: bool = True

    # SQLITE database
    DB_URL: str = "sqlite+aiosqlite:///./app/db/campaign_data_dev.db"


@lru_cache
def get_settings() -> Settings:
    """
    Docstring for get_settings
    
    :return: Description
    :rtype: Settings
    """
    env = config("ENV", cast=str)

    if env == "production":
        print("Loading production settings")
        return ProductionSettings()
    else:
        print("Loading development settings")
        return DevelopmentSettings()
    
settings = get_settings()
