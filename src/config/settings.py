from pathlib import Path
from pydantic import Field
from pydantic import ConfigDict
from pydantic_settings import BaseSettings

_ENV_FILE = Path(__file__).parent.parent.parent / ".env"


class Settings(BaseSettings):
    prefix_app: str       = Field("/v2",        alias="PREFIX_APP")
    env:        str       = Field("dev",        alias="ENV")
    port:       int       = Field(8000,         alias="PORT")
    host:       str       = Field("0.0.0.0",    alias="HOST")

    repository_files_dir: str = Field("/", alias="REPOSITORY_FILES_DIR")

    db_saem3:             str = Field(..., alias="DB_SAEM3")
    db_portabilidad:      str = Field(..., alias="DB_PORTABILIDAD")
    db_masivos_sms:       str = Field(..., alias="DB_MASIVOS_SMS")
    db_telefonos_campanas: str = Field(..., alias="DB_TELEFONOS_CAMPANAS")
    db_email:             str = Field(..., alias="DB_EMAIL")
    db_callb:             str = Field(..., alias="DB_CALLB")

    redis_url: str = Field("redis://localhost:6379/0", alias="REDIS_URL")

    max_campaign_records: int = Field(700_000, alias="MAX_CAMPAIGN_RECORDS")

    use_new_filter: bool = Field(False, alias="USE_NEW_FILTER")
    filter_cache_secret: str = Field("change-me", alias="FILTER_CACHE_SECRET")

    model_config = ConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
