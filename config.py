from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import Enum

class EnvironmentEnum(Enum):
        dev = "dev"
        prod = "prod"

class Settings(BaseSettings):
    ENV: EnvironmentEnum = EnvironmentEnum.dev
    PORT: int = 8000
    HOST: str = "localhost"
    REPOSITORY_FILES_DIR: str = "/"
    DB_SAEM3: str = "mysql+mysqlconnector://user:pass@host:port/database"

    model_config = SettingsConfigDict(
        env_file=".env",
        #extra="allow"  # Permite la existencia de variables extra
    )

settings = Settings()