import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.interfaces.regulation import IRegulation
from modules.process.domain.models.process_dto import DataProcessingDTO


class ValidateRegulations(IPipeline):
    def __init__(self, regulations: list[IRegulation]):
        self.regulations = regulations

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        for regulation in self.regulations:
            regulation.validate(df, ctx)
        return df
