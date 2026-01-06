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
            CleanData(normalizer), # Common
            Exclution(exclusion_source,normalizer), # Common
            AssignOperator(numeration_service), # Call and SMS
            ConcatPrefix(), # Call and SMS
            # AssignCost(), # Call and SMS
            # CustomMessage() #Common
            # Landig() # SMS
            # ValidateRegulations() # Call and SMS 
            # SaveResults() # Common
            # Overview() # Common
        ]

    async def process(self, df: pl.DataFrame, payload: DataProcessingDTO) -> Dict[str, Any]:
        for step in self.steps:
            df = await step.execute(df, payload)

        return {
            "status": "SMS processed",
            "success": True,
        }