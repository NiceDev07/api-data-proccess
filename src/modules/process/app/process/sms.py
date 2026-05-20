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
    Cols.identifier,
]

_ERROR_DESCRIPTIONS: dict[str, str] = {
    ExclusionReason.EXCLUSION_LIST:           "Record found in exclusion list.",
    ExclusionReason.INVALID_NUMBER_LENGTH:    "Number does not meet the required length.",
    ExclusionReason.SHORTNAME_MISSING:        "Message does not contain the required shortname.",
    ExclusionReason.SPECIAL_CHAR_NOT_ALLOWED: "Destination does not allow special characters.",
    ExclusionReason.CHAR_LIMIT_EXCEEDED:      "Message exceeds the allowed character limit.",
}


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

        if sg.total_records == 0:
            reasons = (
                df.filter(pl.col(Cols.error_code).is_not_null())
                .group_by(Cols.error_code)
                .agg(pl.len().alias("affected"))
                .to_dicts()
            )
            logger.warning(
                "SMS completado | válidos: 0 | excluidos: %d | campaña: %s | reasons: %s",
                sg.total_excluded, payload.campaignId,
                [r[Cols.error_code] for r in reasons],
            )
            return {
                "success": False,
                "error": {
                    "code": "NO_VALID_RECORDS",
                    "reasons": [{"code": r[Cols.error_code], "affected": r["affected"]} for r in reasons],
                },
                **summary.model_dump(),
            }

        logger.info(
            "SMS completado | válidos: %d | excluidos: %d | créditos: %g | violaciones: %d",
            sg.total_records, sg.total_excluded, sg.total_credits, len(summary.violations),
        )
        return {"success": True, **summary.model_dump()}

    def _build_summary(self, df: pl.DataFrame) -> CampaignSummary:
        valid = df.filter(pl.col(Cols.is_ok))

        group_df = (
            df.group_by(Cols.cost_operator)
            .agg(
                pl.col(Cols.is_ok).sum().alias("total"),
                (~pl.col(Cols.is_ok)).sum().alias("total_excluded"),
                pl.col(Cols.pdu).filter(pl.col(Cols.is_ok)).sum().alias("pdu"),
                pl.col(Cols.credits).filter(pl.col(Cols.is_ok)).sum().round(3).alias("credits"),
                pl.col(Cols.cost).first().round(3).alias("unit_value"),
            )
            .sort("credits", descending=True)
        )

        general_row = valid.select(
            pl.len().alias("total_records"),
            pl.col(Cols.pdu).sum().alias("total_pdu"),
            pl.col(Cols.credits).sum().round(3).alias("total_credits"),
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
            df.filter(pl.col(Cols.error_code).is_in(list(_ERROR_DESCRIPTIONS)))
            .group_by(Cols.error_code)
            .agg(pl.len().alias("affected"))
        )
        return [
            RegulationViolation(
                code=row[Cols.error_code],
                affected=row["affected"],
                description=_ERROR_DESCRIPTIONS.get(row[Cols.error_code], "Unexpected exclusion."),
            )
            for row in violations_df.to_dicts()
        ]
