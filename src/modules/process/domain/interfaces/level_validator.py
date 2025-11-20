# modules/process/domain/interfaces/user_level_validator.py
from typing import Protocol
from modules.process.domain.models.process_dto import DataProcessingDTO
import polars as pl

class IUserLevelValidator(Protocol):
    async def validate(self, df: pl.DataFrame, payload: DataProcessingDTO) -> None: ...
