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

    DASK_N_WORKERS: int = 6
    DASK_THREADS_PER_WORKER: int = 4
    DASK_MEMORY_LIMIT: str = "2GB"
    DASK_PROCESSES: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        #extra="allow"  # Permite la existencia de variables extra
    )

settings = Settings()