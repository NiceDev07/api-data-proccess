from typing import Protocol
import polars as pl
from ..models.process_dto import BaseFileConfig

class IFileReader(Protocol):
    async def read(self, file: BaseFileConfig) -> pl.LazyFrame: ...
