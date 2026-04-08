from typing import Dict, Any
import polars as pl

from modules.process.domain.interfaces.process import IDataProcessor
from modules.process.domain.interfaces.storage import IStorage
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.models.summary import EmailCampaignSummary, EmailSummaryGeneral, EmailSummaryGroup
from modules.process.domain.constants.cols import Cols
from modules.process.app.normalizers.email import EmailNormalizer
from modules.process.app.pipelines import CustomMessage, SaveResults
from modules.process.app.pipelines.custom_subject import CustomSubject
from modules.process.app.pipelines.assign_cost_email import AssignCostEmail
from modules.process.app.pipelines.calculate_credits_email import CalculateCreditsEmail
from modules.process.app.pipelines.clean_data_email import CleanDataEmail
from modules.process.app.pipelines.exclution_email import ExclutionEmail
from modules.process.app.pipelines.extract_email_domain import ExtractEmailDomain
from modules.process.app.pipelines.validate_email import ValidateEmail
from logging_config import get_logger

logger = get_logger(__name__)

_EMAIL_COLS = [
    Cols.email,
    Cols.message,
    Cols.subject,
    Cols.cost,
    Cols.credits,
    Cols.email_domain,
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

    async def process(self, df: pl.DataFrame, payload: DataProcessingDTO) -> Dict[str, Any]:
        logger.info(
            "Email iniciado | campaña: %s | registros: %d",
            payload.campaignId, df.height,
        )
        for step in self._pre_steps:
            df = await step.execute(df, payload)

        # Cadena lazy: 6 with_columns sin materializar hasta SaveResults
        lf = df.lazy()
        for step in self._transform_steps:
            lf = await step.execute(lf, payload)

        df = await self._save_step.execute(lf, payload)

        summary = self._build_summary(df)
        sg = summary.summaryGeneral
        logger.info(
            "Email completado | válidos: %d | excluidos: %d | créditos: %.4f",
            sg.total_records, sg.total_excluded, sg.total_credits,
        )
        return {"success": True, **summary.model_dump()}

    def _build_summary(self, df: pl.DataFrame) -> EmailCampaignSummary:
        valid = df.filter(pl.col(Cols.is_ok))

        valid = valid.with_columns(
            pl.when(pl.col(Cols.email_domain).is_in(_KNOWN_DOMAINS))
            .then(pl.col(Cols.email_domain))
            .otherwise(pl.lit(_OTHER_DOMAIN))
            .alias(Cols.email_domain)
        )

        group_df = (
            valid.group_by(Cols.email_domain)
            .agg(
                pl.len().alias("total"),
                pl.col(Cols.cost).first().alias("unit_value"),
                pl.col(Cols.credits).sum().alias("credits"),
            )
            .sort("credits", descending=True)
        )

        general_row = valid.select(
            pl.len().alias("total_records"),
            pl.col(Cols.credits).sum().alias("total_credits"),
        ).row(0, named=True)

        return EmailCampaignSummary(
            summaryGroup=[
                EmailSummaryGroup(**r)
                for r in group_df.rename({Cols.email_domain: "domain"}).to_dicts()
            ],
            summaryGeneral=EmailSummaryGeneral(
                **general_row,
                total_excluded=df.height - valid.height,
            ),
        )
