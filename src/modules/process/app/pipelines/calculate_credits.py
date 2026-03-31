import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols


class CalculateCredits(IPipeline):
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        return df.with_columns(
            (pl.col(Cols.pdu) * pl.col(Cols.cost))
            .cast(pl.Float64)
            .alias(Cols.credits)
        )
