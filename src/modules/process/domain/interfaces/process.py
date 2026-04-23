from typing import Protocol, Dict, Any
from ..models.process_dto import DataProcessingDTO
import polars as pl

class IDataProcessor(Protocol):
    async def process(self, lf: pl.LazyFrame, payload: DataProcessingDTO) -> Dict[str, Any]: ...