from typing import Protocol, Dict, Any
from ..models.process_dto import DataProcessingDTO
import polars as pl

class IDataProcessor(Protocol):
    async def process(self, df: pl.DataFrame, payload: DataProcessingDTO) -> Dict[str, Any]: ...