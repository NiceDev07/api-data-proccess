import polars as pl
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.ports.exclusion_source import IExclusionSource
from modules.process.domain.interfaces.normalizer import INormalizer

class Exclution(IPipeline):
    def __init__(
        self,
        exclusion_source: IExclusionSource,
        normalizer: INormalizer,
    ):
        self.exclusion_source = exclusion_source
        self.normalizer = normalizer
        

    async def execute(
        self,
        df: pl.DataFrame,
        ctx: DataProcessingDTO
    ) -> pl.DataFrame:
        from modules.process.domain.constants.cols import Cols
        from modules.process.domain.constants.reasons import ExclusionReason

        if not ctx.useExclusionList or ctx.configListExclusion is None:
            return df.with_columns(
                pl.lit(True).alias(Cols.is_ok),
                pl.lit(None).cast(pl.Utf8).alias(Cols.error_code),
            )

        c = ctx.configListExclusion.nameColumnDemographic
        colum_main = ctx.configFile.nameColumnDemographic
        numbers_to_exclude = await self.exclusion_source.get_df(ctx)

        if numbers_to_exclude.is_empty():
            return df.with_columns(
                pl.lit(True).alias(Cols.is_ok),
                pl.lit(None).cast(pl.Utf8).alias(Cols.error_code),
            )

        cleaned = self.normalizer.normalize(c)
        numbers_to_exclude = (
            numbers_to_exclude.with_columns(cleaned)
              .filter(pl.col(c).is_not_null())
              .with_columns(pl.col(c).cast(pl.Int64, strict=False).alias(c))
        )

        exclusion_series = numbers_to_exclude.get_column(c).implode()
        is_excluded = pl.col(colum_main).is_in(exclusion_series)
        result = df.with_columns(
            (~is_excluded).alias(Cols.is_ok),
            pl.when(is_excluded)
            .then(pl.lit(ExclusionReason.EXCLUSION_LIST))
            .otherwise(pl.lit(None).cast(pl.Utf8))
            .alias(Cols.error_code),
        )
        return result
    