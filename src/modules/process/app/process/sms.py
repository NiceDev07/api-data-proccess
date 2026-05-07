from typing import Dict, Any
import polars as pl

from modules.process.domain.interfaces.process import IDataProcessor
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.app.pipelines import (
    CleanData, ConcatPrefix, AssignOperator, AssignCost, CalculateCredits,
    CalculatePDU, CustomMessage, Exclution, Landing, SaveResults, ValidateRegulations,
)
from modules.process.app.normalizers.number import NumberNormalizer
from modules.process.app.regulations.sms import SMS_REGULATIONS
from modules.process.domain.interfaces.storage import IStorage
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason
from modules.process.domain.models.summary import (
    CampaignSummary, RegulationViolation, SummaryGeneral, SummaryGroup,
)
from logging_config import get_logger

logger = get_logger(__name__)

_SMS_COLS = [
    Cols.number_concat,
    Cols.message,
    Cols.number_operator,
    Cols.pdu,
    Cols.credits,
]

_REGULATION_DESCRIPTIONS: dict[str, str] = {
    ExclusionReason.SHORTNAME_MISSING:       "El mensaje no contiene el shortname requerido",
    ExclusionReason.SPECIAL_CHAR_NOT_ALLOWED: "El país de destino no permite caracteres especiales",
    ExclusionReason.CHAR_LIMIT_EXCEEDED:     "El mensaje supera el límite de caracteres permitido",
}

_REGULATION_CODES = frozenset(_REGULATION_DESCRIPTIONS)


class SmsProcessor(IDataProcessor):
    def __init__(self, numeration_service, exclusion_source, cost_service, storage: IStorage):
        normalizer = NumberNormalizer()
        self.steps = [
            CleanData(normalizer),
            Exclution(exclusion_source, normalizer),
            AssignOperator(numeration_service),
            ConcatPrefix(),
            AssignCost(cost_service, service="sms"),
            CustomMessage(),
            Landing(),
            CalculatePDU(),
            ValidateRegulations(SMS_REGULATIONS),
            CalculateCredits(),
            SaveResults(_SMS_COLS, storage, service="sms"),
        ]

    async def process(self, lf: pl.LazyFrame | pl.DataFrame, payload: DataProcessingDTO) -> Dict[str, Any]:
        df = lf.collect(engine="streaming") if isinstance(lf, pl.LazyFrame) else lf
        logger.info(
            "SMS iniciado | campaña: %s | registros: %d",
            payload.campaignId, df.height,
        )
        for step in self.steps:
            df = await step.execute(df, payload)

        summary = self._build_summary(df)
        sg = summary.summaryGeneral
        logger.info(
            "SMS completado | válidos: %d | excluidos: %d | créditos: %.4f | violaciones: %d",
            sg.total_records, sg.total_excluded, sg.total_credits, len(summary.violations),
        )
        return {"success": True, **summary.model_dump()}

    def _build_summary(self, df: pl.DataFrame) -> CampaignSummary:
        valid = df.filter(pl.col(Cols.is_ok))

        group_df = (
            valid.group_by(Cols.cost_operator)
            .agg(
                pl.len().alias("total"),
                pl.col(Cols.pdu).sum().alias("pdu"),
                pl.col(Cols.credits).sum().alias("credits"),
                pl.col(Cols.cost).first().alias("unit_value"),
            )
            .sort("credits", descending=True)
        )

        general_row = valid.select(
            pl.len().alias("total_records"),
            pl.col(Cols.pdu).sum().alias("total_pdu"),
            pl.col(Cols.credits).sum().alias("total_credits"),
        ).row(0, named=True)

        violations = self._build_violations(df)

        return CampaignSummary(
            summaryGroup=[
                SummaryGroup(**r)
                for r in group_df.rename({Cols.cost_operator: "operator"}).to_dicts()
            ],
            summaryGeneral=SummaryGeneral(
                **general_row,
                total_excluded=df.height - valid.height,
            ),
            violations=violations,
        )

    def _build_violations(self, df: pl.DataFrame) -> list[RegulationViolation]:
        violations_df = (
            df.filter(pl.col(Cols.error_code).is_in(_REGULATION_CODES))
            .group_by(Cols.error_code)
            .agg(pl.len().alias("affected"))
        )
        return [
            RegulationViolation(
                code=row[Cols.error_code],
                affected=row["affected"],
                description=_REGULATION_DESCRIPTIONS.get(row[Cols.error_code], row[Cols.error_code]),
            )
            for row in violations_df.to_dicts()
        ]
