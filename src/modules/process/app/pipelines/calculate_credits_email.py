import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols


class CalculateCreditsEmail(IPipeline):
    # Email: 1 registro = 1 costo unitario (sin multiplicador adicional)
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        return df.with_columns(
            pl.col(Cols.cost).cast(pl.Float64).round(3).alias(Cols.credits)
        )
