from typing import Dict, Any
import polars as pl
from modules.process.domain.interfaces.process import IDataProcessor
from modules.process.domain.interfaces.storage import IStorage
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.models.summary import EmailCampaignSummary, EmailSummaryGeneral, EmailSummaryGroup
from modules.process.domain.constants.cols import Cols
from modules.process.app.normalizers.email import EmailNormalizer
from modules.process.app.helpers import attach_identifier
from modules.process.app.pipelines import (
    CustomMessage, SaveResults,
    CustomSubject, AssignCostEmail, CalculateCreditsEmail,
    CleanDataEmail, ExclutionEmail, ExtractEmailDomain, ValidateEmail,
)
from logging_config import get_logger

logger = get_logger(__name__)

_EMAIL_COLS = [
    Cols.email,
    Cols.message,
    Cols.subject,
    Cols.cost,
    Cols.credits,
    Cols.email_domain,
    Cols.identifier,
]

_KNOWN_DOMAINS = frozenset([
    "gmail.com",
    "hotmail.com",
    "yahoo.com",
    "outlook.com",
    "icloud.com",
])

_OTHER_DOMAIN = "others"


class EmailProcessor(IDataProcessor):
    def __init__(self, exclusion_source, cost_service, storage: IStorage):
        normalizer = EmailNormalizer()
        # Pasos que necesitan materialización (filtros, exclusiones con .to_list())
        self._pre_steps = [
            CleanDataEmail(normalizer),
            ExclutionEmail(exclusion_source, normalizer),
        ]
        # Pasos puros: solo with_columns — se ejecutan como cadena lazy
        self._transform_steps = [
            ValidateEmail(),
            ExtractEmailDomain(),
            AssignCostEmail(cost_service),
            CustomMessage(),
            CustomSubject(),
            CalculateCreditsEmail(),
        ]
        self._save_step = SaveResults(_EMAIL_COLS, storage, service="email")

    async def process(self, lf: pl.LazyFrame | pl.DataFrame, payload: DataProcessingDTO) -> Dict[str, Any]:
        logger.info("Email iniciado | campaña: %s", payload.campaignId)
        if not isinstance(lf, pl.LazyFrame):
            lf = lf.lazy()

        # Adjunta el identificador de usuario antes de los pasos del pipeline
        lf = attach_identifier(lf, payload)

        # Pre_steps (filter + rename): streaming añade overhead en planes simples;
        # se materializan con collect() estándar sobre datos ya en memoria.
        for step in self._pre_steps:
            lf = await step.execute(lf, payload)
        df = lf.collect()
        logger.debug(
            "Email pre-steps completados | registros tras limpieza y exclusión: %d",
            df.height,
        )

        # Transform chain: 6 with_columns puros sin filter ni rename →
        # collect(streaming) gana ~45% en 1M filas al procesar en chunks.
        lf = df.lazy()
        for step in self._transform_steps:
            lf = await step.execute(lf, payload)

        df = await self._save_step.execute(lf, payload)

        summary = self._build_summary(df)
        sg = summary.summaryGeneral
        logger.info(
            "Email completado | válidos: %d | excluidos: %d | créditos: %g",
            sg.total_records, sg.total_excluded, sg.total_credits,
        )
        return {"success": True, **summary.model_dump()}

    def _build_summary(self, df: pl.DataFrame) -> EmailCampaignSummary:
        valid_lf = df.lazy().filter(pl.col(Cols.is_ok))

        normalized_domain = (
            pl.when(pl.col(Cols.email_domain).is_in(list(_KNOWN_DOMAINS)))
            .then(pl.col(Cols.email_domain))
            .otherwise(pl.lit(_OTHER_DOMAIN))
        )

        general_lf = valid_lf.select(
            pl.len().alias("total_records"),
            pl.col(Cols.credits).sum().round(3).alias("total_credits"),
        )
        group_lf = (
            df.lazy()
            .with_columns(normalized_domain.alias(Cols.email_domain))
            .group_by(Cols.email_domain)
            .agg(
                pl.col(Cols.is_ok).sum().alias("total"),
                (~pl.col(Cols.is_ok)).sum().alias("total_excluded"),
                pl.col(Cols.cost).first().round(3).alias("unit_value"),
                pl.col(Cols.credits).filter(pl.col(Cols.is_ok)).sum().round(3).alias("credits"),
            )
            .sort("credits", descending=True)
        )

        general_df, group_df = pl.collect_all([general_lf, group_lf])
        general_row = general_df.row(0, named=True)

        return EmailCampaignSummary(
            summaryGroup=[
                EmailSummaryGroup(**r)
                for r in group_df.rename({Cols.email_domain: "domain"}).to_dicts()
            ],
            summaryGeneral=EmailSummaryGeneral(
                **general_row,
                total_excluded=df.height - general_row["total_records"],
            ),
        )
