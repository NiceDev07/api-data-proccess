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
        c = ctx.configListExclusion.nameColumnDemographic
        colum_main = ctx.configFile.nameColumnDemographic
        numbers_to_exclude = await self.exclusion_source.get_df(ctx)

        if numbers_to_exclude.is_empty():
            return df  # no excluye nada

        cleaned = self.normalizer.normalize(c)
        numbers_to_exclude = (
            numbers_to_exclude.with_columns(cleaned)
              .filter(
                  pl.col(c).is_not_null()
              )
              .with_columns(pl.col(c).cast(pl.Int64, strict=False).alias(c))
        )

        return df.filter(
            ~pl.col(colum_main).is_in(numbers_to_exclude.get_column(c))
        )
    