from typing import Dict, Any
from modules.process.domain.interfaces.process import IDataProcessor
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.data_processing.application.pipelines import (CleanData, ConcatPrefix)
import polars as pl

class SmsProcessor(IDataProcessor):
    def __init__(self):
        self.steps = [
            CleanData(),
            ConcatPrefix(),
        ]

    async def process(self, df: pl.DataFrame, payload: DataProcessingDTO) -> Dict[str, Any]:
        return {
            "status": "SMS processed",
            "success": True,
        }