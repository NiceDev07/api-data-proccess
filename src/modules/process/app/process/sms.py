from typing import Dict, Any
from modules.process.domain.interfaces.process import IDataProcessor
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.app.pipelines import (CleanData, ConcatPrefix, AssignOperator, Exclution)
from modules.process.app.normalizers.number import NumberNormalizer
import polars as pl

class SmsProcessor(IDataProcessor):
    def __init__(self,
        numeration_service,
        exclusion_source
    ):
        normalizer = NumberNormalizer()
        self.steps = [
            CleanData(normalizer),
            Exclution(exclusion_source,normalizer), # DEBE LIMPIAR ANTES CLEAN DATA
            ConcatPrefix(),
            AssignOperator(numeration_service)
        ]

    async def process(self, df: pl.DataFrame, payload: DataProcessingDTO) -> Dict[str, Any]:
    
        for step in self.steps:
            df = await step.execute(df, payload)
            print(df)

        return {
            "status": "SMS processed",
            "success": True,
        }