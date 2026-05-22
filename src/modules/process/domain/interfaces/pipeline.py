from abc import ABC, abstractmethod
import polars as pl
from modules.process.domain.models.process_dto import DataProcessingDTO


class IPipeline(ABC):
    @abstractmethod
    async def execute(self, df: pl.DataFrame | pl.LazyFrame, ctx: DataProcessingDTO) -> pl.DataFrame | pl.LazyFrame:
        pass