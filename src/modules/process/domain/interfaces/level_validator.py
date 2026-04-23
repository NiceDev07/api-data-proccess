from typing import Protocol
from modules.process.domain.models.process_dto import DataProcessingDTO
import polars as pl

class IUserLevelValidator(Protocol):
    async def validate(self, lf: pl.LazyFrame, payload: DataProcessingDTO) -> pl.LazyFrame: ...
