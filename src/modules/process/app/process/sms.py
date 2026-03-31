from typing import Dict, Any
from modules.process.domain.interfaces.process import IDataProcessor
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.app.pipelines import (CleanData, ConcatPrefix, AssignOperator, AssignCost, CalculateCredits, CalculatePDU, CustomMessage, Exclution, Landing, SaveResults, ValidateRegulations)
from modules.process.app.normalizers.number import NumberNormalizer
from modules.process.app.regulations.sms import SMS_REGULATIONS
from modules.process.domain.interfaces.storage import IStorage
from modules.process.domain.constants.cols import Cols
from modules.process.domain.models.summary import CampaignSummary, SummaryGeneral, SummaryGroup
import polars as pl

_SMS_COLS = [
    Cols.number_concat,
    Cols.message,
    Cols.number_operator,
    Cols.pdu,
    Cols.credits,
]

class SmsProcessor(IDataProcessor):
    def __init__(self,
        numeration_service,
        exclusion_source,
        cost_service,
        storage: IStorage,
    ):
        normalizer = NumberNormalizer()
        self.steps = [
            CleanData(normalizer), # Common
            Exclution(exclusion_source,normalizer), # Common
            AssignOperator(numeration_service), # Call and SMS
            ConcatPrefix(), # Call and SMS
            AssignCost(cost_service), # Call and SMS
            CustomMessage(), # Common
            Landing(), # SMS
            CalculatePDU(), # Common
            ValidateRegulations(SMS_REGULATIONS), # SMS
            CalculateCredits(), # Common
            SaveResults(_SMS_COLS, storage), # Common
            # Overview() # Common
        ]

    async def process(self, df: pl.DataFrame, payload: DataProcessingDTO) -> Dict[str, Any]:
        for step in self.steps:
            df = await step.execute(df, payload)
            print(df)
        return {"success": True, **self._build_summary(df).model_dump()}

    def _build_summary(self, df: pl.DataFrame) -> CampaignSummary:
        valid = df.filter(pl.col(Cols.is_ok))

        group_df = (
            valid.group_by(Cols.cost_operator)
            .agg(
                pl.len().alias("total"),
                pl.col(Cols.pdu).sum().alias("pdu"),
                pl.col(Cols.credits).sum().alias("credits"),
            )
            .with_columns(
                (pl.col("credits") / pl.col("total")).alias("unit_value")
            )
            .sort("credits", descending=True)
        )

        general_row = valid.select(
            pl.len().alias("total_records"),
            pl.col(Cols.pdu).sum().alias("total_pdu"),
            pl.col(Cols.credits).sum().alias("total_credits"),
        ).row(0, named=True)

        return CampaignSummary(
            summaryGroup=[SummaryGroup(**r) for r in group_df.rename({Cols.cost_operator: "operator"}).to_dicts()],
            summaryGeneral=SummaryGeneral(
                **general_row,
                total_excluded=df.height - valid.height,
            ),
        )