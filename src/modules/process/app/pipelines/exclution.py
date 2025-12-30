import polars as pl
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.ports.exclusion_source import IExclusionSource

class Exclution(IPipeline):
    def __init__(
        self,
        exclusion_source: IExclusionSource
    ):
        self.exclusion_source = exclusion_source

    async def execute(
        self, 
        df: pl.DataFrame,
        ctx: DataProcessingDTO
    ) -> pl.DataFrame:
        numbers_to_exclude = await self.exclusion_source.get_df(ctx)

        if not numbers_to_exclude:
            return df  # no excluye nada

        return df.filter(
            ~pl.col(self.column_name).is_in(numbers_to_exclude)
        )
    