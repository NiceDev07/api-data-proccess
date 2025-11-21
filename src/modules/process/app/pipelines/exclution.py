from src.modules.process.domain.interfaces.pipeline import IPipeline
from src.modules.process.domain.interfaces.exclusion_source import IExclusionSource
import polars as pl

class Exclution(IPipeline):
    def __init__(
        self,
        exclusion_source: IExclusionSource
    ):
        self.exclusion_source = exclusion_source

    async def execute(
        self,
        df: pl.DataFrame
    ) -> pl.DataFrame:
        numbers_to_exclude = await self.exclusion_source.get_numbers()

        if not numbers_to_exclude:
            return df  # no excluye nada

        return df.filter(
            ~pl.col(self.column_name).is_in(numbers_to_exclude)
        )
    