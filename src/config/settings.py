from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import Enum

class EnvironmentEnum(Enum):
        dev = "dev"
        prod = "prod"

class Settings(BaseSettings):
    PREFIX_APP: str = "/v2"
    ENV: EnvironmentEnum = EnvironmentEnum.dev.value
    PORT: int = 8000
    HOST: str = "localhost"
    REPOSITORY_FILES_DIR: str = "/"

    DB_SAEM3: str = "mysql+mysqlconnector://user:pass@host:port/database"
    DB_PORTABILIDAD: str = "mysql+mysqlconnector://user:pass@host:port/database"
    DB_MASIVOS_SMS: str = "mysql+mysqlconnector://user:pass@host:port/database"
    REDIS_URL: str = "redis://localhost:6379/0"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"  # Permite la existencia de variables extra
    )

settings = Settings()