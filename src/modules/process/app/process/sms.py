from typing import Dict, Any
import polars as pl

from modules.process.domain.interfaces.process import IDataProcessor
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.app.pipelines import (
    CleanData, ConcatPrefix, AssignCost, CalculateCredits,
    CalculatePDU, CustomMessage, Exclution, Landing, SaveResults, ValidateRegulations,
    ValidateForbiddenWordsStep,
)
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.app.helpers import attach_identifier, build_no_valid_records_response
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
    ExclusionReason.NO_OPERATOR:              "Number not assignable to any operator range.",
    ExclusionReason.INVALID_NUMBER_LENGTH:    "Number does not meet the required length.",
    ExclusionReason.SHORTNAME_MISSING:        "Message does not contain the required shortname.",
    ExclusionReason.SPECIAL_CHAR_NOT_ALLOWED: "Destination does not allow special characters.",
    ExclusionReason.URL_REQUIRED:             "Message must include a URL for landing sub-service.",
    # ExclusionReason.CHAR_LIMIT_EXCEEDED:      "Message exceeds the allowed character limit.",
}


class SmsProcessor(IDataProcessor):
    def __init__(
        self,
        operator_step: IPipeline,
        exclusion_source,
        cost_service,
        storage: IStorage,
        forbidden_words_service=None,
    ):
        normalizer = NumberNormalizer()
        # `operator_step` se inyecta por flujo (DIP), sin ramas condicionales:
        # - masivo: AssignOperator → numeración vectorizada, sin portabilidad.
        # - unitario: AssignOperatorRouting → SP consulta_operador_pais (operador +
        #   routing + PORTABILIDAD por número). Ver process_deps.
        # Pasos de TRANSFORMACIÓN (compartidos). El terminal (SaveResults) se separa
        # para reutilizar el pipeline con otra salida: masivo → Parquet + resumen;
        # unitario → filas por número (process_rows).
        self.steps = [
            CleanData(normalizer),
            Exclution(exclusion_source, normalizer),
            operator_step,
            ConcatPrefix(),
            AssignCost(cost_service, service="sms"),
            CustomMessage(),
            Landing(),
            CalculatePDU(),
            ValidateRegulations(SMS_REGULATIONS),
            CalculateCredits(),
        ]
        self.sink = SaveResults(_SMS_COLS, storage, service="sms")
        # Solo MASIVO: el unitario ya valida palabras prohibidas vía el endpoint HTTP
        # /filtro/validar que llama el gateway antes de invocar este pipeline (ver
        # docs/forbidden-words/README.md) — meterlo aquí también duplicaría la validación.
        self._forbidden_words_step = (
            ValidateForbiddenWordsStep(forbidden_words_service)
            if forbidden_words_service is not None else None
        )

    async def _run_steps(self, df: pl.DataFrame, payload: DataProcessingDTO, steps: list[IPipeline]) -> pl.DataFrame:
        for step in steps:
            try:
                df = await step.execute(df, payload)
            except Exception as exc:
                step_name = type(step).__name__
                logger.error("SMS | error en paso %s: %s", step_name, exc)
                raise
        return df

    async def _collect(self, lf: pl.LazyFrame | pl.DataFrame, payload: DataProcessingDTO) -> pl.DataFrame:
        df = attach_identifier(
            lf if isinstance(lf, pl.LazyFrame) else lf.lazy(), payload
        ).collect(engine="streaming")
        logger.info(
            "SMS iniciado | campaña: %s | registros: %d",
            payload.campaignId, df.height,
        )
        return df

    async def process_rows(self, lf: pl.LazyFrame | pl.DataFrame, payload: DataProcessingDTO) -> pl.DataFrame:
        # Flujo UNITARIO: mismo pipeline, salida por-número (sin Parquet, sin resumen,
        # sin paso de palabras prohibidas — ya lo validó el gateway antes de llegar aquí).
        df = await self._collect(lf, payload)
        return await self._run_steps(df, payload, self.steps)

    async def process(self, lf: pl.LazyFrame | pl.DataFrame, payload: DataProcessingDTO) -> Dict[str, Any]:
        # Flujo MASIVO: palabras prohibidas primero, luego transforma → persiste → resumen.
        df = await self._collect(lf, payload)
        if self._forbidden_words_step is not None:
            df = await self._run_steps(df, payload, [self._forbidden_words_step])
        df = await self._run_steps(df, payload, self.steps)
        df = await self.sink.execute(df, payload)

        summary = self._build_summary(df)
        sg = summary.summaryGeneral

        if sg.total_records == 0:
            return build_no_valid_records_response(
                df=df, summary_dump=summary.model_dump(),
                service="SMS", payload=payload, total_excluded=sg.total_excluded,
            )

        logger.info(
            "SMS completado | válidos: %d | excluidos: %d | créditos: %g | violaciones: %d",
            sg.total_records, sg.total_excluded, sg.total_credits, len(summary.violations),
        )
        return {"success": True, **summary.model_dump()}

    def _build_summary(self, df: pl.DataFrame) -> CampaignSummary:
        valid = df.filter(pl.col(Cols.is_ok))
        excluded = df.filter(~pl.col(Cols.is_ok))

        group_df = (
            df.group_by(Cols.cost_operator)
            .agg(
                pl.col(Cols.is_ok).sum().alias("total"),
                (~pl.col(Cols.is_ok)).sum().alias("total_excluded"),
                pl.col(Cols.pdu).filter(pl.col(Cols.is_ok)).sum().alias("pdu"),
                pl.col(Cols.credits).filter(pl.col(Cols.is_ok)).sum().round(3).alias("credits"),
            )
            .with_columns(
                # unit_value: promedio real de créditos por registro válido del grupo.
                # fill_nan(0) cubre el caso donde total=0 (todos excluidos en el grupo).
                (pl.col("credits") / pl.col("total")).fill_nan(0.0).round(3).alias("unit_value")
            )
            .sort("credits", descending=True)
        )

        general_row = valid.select(
            pl.len().alias("total_records"),
            pl.col(Cols.pdu).sum().alias("total_pdu"),
            pl.col(Cols.credits).sum().round(3).alias("total_credits"),
        ).row(0, named=True)

        violations = self._build_violations(excluded)

        return CampaignSummary(
            summaryGroup=[
                SummaryGroup(**r)
                for r in group_df.rename({Cols.cost_operator: "operator"}).to_dicts()
            ],
            summaryGeneral=SummaryGeneral(
                **general_row,
                total_excluded=excluded.height,
            ),
            violations=violations,
        )

    def _build_violations(self, excluded: pl.DataFrame) -> list[RegulationViolation]:
        # Recibe solo los registros excluidos — evita filtrar el DF completo dos veces
        violations_df = (
            excluded.filter(pl.col(Cols.error_code).is_in(list(_ERROR_DESCRIPTIONS)))
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
