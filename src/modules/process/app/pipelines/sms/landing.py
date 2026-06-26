import polars as pl

from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason
from modules.process.domain.enums.sub_services import SmsSubService
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO

_URL_PATTERN = r"https?://\S+"


class Landing(IPipeline):
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        # Solo aplica a sub-servicio landing.
        if ctx.subService != SmsSubService.landing:
            return df

        # Valida sobre el mensaje ya reemplazado por CustomMessage para permitir que
        # la URL venga dentro de un {tag} del CSV. Los registros sin URL quedan
        # excluidos con URL_REQUIRED en vez de abortar la campaña entera.
        has_url = pl.col(Cols.message).str.contains(_URL_PATTERN)
        to_invalidate = pl.col(Cols.is_ok) & ~has_url

        return df.with_columns(
            pl.when(to_invalidate).then(pl.lit(False)).otherwise(pl.col(Cols.is_ok)).alias(Cols.is_ok),
            pl.when(to_invalidate)
              .then(pl.lit(ExclusionReason.URL_REQUIRED))
              .otherwise(pl.col(Cols.error_code))
              .alias(Cols.error_code),
        )
