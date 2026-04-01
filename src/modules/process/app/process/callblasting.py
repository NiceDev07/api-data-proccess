from typing import Dict, Any
import polars as pl

from modules.process.domain.interfaces.audio_duration_provider import IAudioDurationProvider
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.interfaces.process import IDataProcessor
from modules.process.domain.interfaces.storage import IStorage
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.models.summary import CBCampaignSummary, CBSummaryGeneral, CBSummaryGroup
from modules.process.domain.constants.cols import Cols
from modules.process.domain.enums.sub_services import CallBlastingSubService
from modules.process.app.normalizers.number import NumberNormalizer
from modules.process.app.pipelines import (
    CleanData, Exclution, ValidatePhoneLength, AssignOperator, ConcatPrefix,
    AssignCostCallBlasting,
    CalculateDurationStandard, CalculateDurationCustom,
    CustomMessage, CalculateCreditsCallBlasting, SaveResults,
)

# Columnas que se guardan por sub-servicio
_OUTPUT_COLS: dict[CallBlastingSubService, list[str]] = {
    CallBlastingSubService.standard: [
        Cols.number_concat,
        Cols.number_operator,
        Cols.seconds,
        Cols.credits,
    ],
    CallBlastingSubService.custom: [
        Cols.number_concat,
        Cols.number_operator,
        Cols.message,
        Cols.seconds,
        Cols.credits,
    ],
}


class CallBlastingProcessor(IDataProcessor):
    def __init__(
        self,
        numeration_service,
        exclusion_source,
        cost_service,
        storage: IStorage,
        duration_provider: IAudioDurationProvider,
    ):
        normalizer = NumberNormalizer()

        # Pasos compartidos por todos los sub-servicios
        common = [
            CleanData(normalizer),
            Exclution(exclusion_source, normalizer),
            ValidatePhoneLength(),
            AssignOperator(numeration_service),
            ConcatPrefix(),
            AssignCostCallBlasting(cost_service),
        ]

        # Registro: sub-servicio → pasos exclusivos
        # Para agregar un nuevo sub-servicio: añadir una entrada aquí y en _OUTPUT_COLS
        self._pipelines: dict[CallBlastingSubService, list[IPipeline]] = {
            CallBlastingSubService.standard: common + [
                CalculateDurationStandard(duration_provider),
                CalculateCreditsCallBlasting(),
                SaveResults(_OUTPUT_COLS[CallBlastingSubService.standard], storage),
            ],
            CallBlastingSubService.custom: common + [
                CustomMessage(),
                CalculateDurationCustom(),
                CalculateCreditsCallBlasting(),
                SaveResults(_OUTPUT_COLS[CallBlastingSubService.custom], storage),
            ],
        }

    async def process(self, df: pl.DataFrame, payload: DataProcessingDTO) -> Dict[str, Any]:
        steps = self._pipelines.get(payload.subService)
        if steps is None:
            raise ValueError(f"Sub-servicio de call blasting no soportado: {payload.subService}")

        for step in steps:
            df = await step.execute(df, payload)

        return {"success": True, **self._build_summary(df).model_dump()}

    def _build_summary(self, df: pl.DataFrame) -> CBCampaignSummary:
        valid = df.filter(pl.col(Cols.is_ok))

        group_df = (
            valid.group_by(Cols.cost_operator)
            .agg(
                pl.len().alias("total"),
                pl.col(Cols.seconds).sum().alias("seconds"),
                pl.col(Cols.credits).sum().alias("credits"),
            )
            .with_columns(
                (pl.col("credits") / pl.col("total")).alias("unit_value")
            )
            .sort("credits", descending=True)
        )

        general_row = valid.select(
            pl.len().alias("total_records"),
            pl.col(Cols.seconds).sum().alias("total_seconds"),
            pl.col(Cols.credits).sum().alias("total_credits"),
        ).row(0, named=True)

        return CBCampaignSummary(
            summaryGroup=[
                CBSummaryGroup(**r)
                for r in group_df.rename({Cols.cost_operator: "operator"}).to_dicts()
            ],
            summaryGeneral=CBSummaryGeneral(
                **general_row,
                total_excluded=df.height - valid.height,
            ),
        )
