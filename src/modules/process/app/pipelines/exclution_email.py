import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.ports.exclusion_source import IExclusionSource
from modules.process.domain.interfaces.normalizer import INormalizer
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason
from logging_config import get_logger

logger = get_logger(__name__)


class ExclutionEmail(IPipeline):
    def __init__(self, exclusion_source: IExclusionSource, normalizer: INormalizer):
        self.exclusion_source = exclusion_source
        self.normalizer = normalizer

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        if not ctx.useExclusionList or ctx.configListExclusion is None:
            logger.debug("ExclutionEmail | lista de exclusión desactivada — todos los registros marcados OK")
            return df.with_columns(
                pl.lit(True).alias(Cols.is_ok),
                pl.lit(None).cast(pl.Utf8).alias(Cols.error_code),
            )

        c = ctx.configListExclusion.nameColumnDemographic
        emails_to_exclude = await self.exclusion_source.get_df(ctx)

        if emails_to_exclude.is_empty():
            logger.debug("ExclutionEmail | lista de exclusión vacía — todos los registros marcados OK")
            return df.with_columns(
                pl.lit(True).alias(Cols.is_ok),
                pl.lit(None).cast(pl.Utf8).alias(Cols.error_code),
            )

        logger.debug("ExclutionEmail | emails en lista de exclusión cargados: %d", emails_to_exclude.height)

        emails_to_exclude = (
            emails_to_exclude
            .with_columns(self.normalizer.normalize(c))
            .filter(pl.col(c).is_not_null())
        )

        # .implode() required: is_in(Series[same_dtype]) is ambiguous in Polars 1.31+
        exclusion_series = emails_to_exclude.get_column(c).implode()
        is_excluded = pl.col(Cols.email).is_in(exclusion_series)

        result = df.with_columns(
            (~is_excluded).alias(Cols.is_ok),
            pl.when(is_excluded)
            .then(pl.lit(ExclusionReason.EXCLUSION_LIST))
            .otherwise(pl.lit(None).cast(pl.Utf8))
            .alias(Cols.error_code),
        )

        # La cuenta de excluidos solo es precisa si df es un DataFrame materializado
        if isinstance(result, pl.DataFrame):
            excluidos = result.filter(~pl.col(Cols.is_ok)).height
            logger.debug("ExclutionEmail | registros excluidos por lista: %d de %d", excluidos, df.height)

        return result
