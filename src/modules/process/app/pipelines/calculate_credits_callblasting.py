import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols


class CalculateCreditsCallBlasting(IPipeline):
    """Calcula créditos para call blasting.

    Fórmula:
        si seconds > initial → cycles = ⌈seconds / incremental⌉
        si no               → cycles = initial   (mínimo de facturación)
        credits = cycles × incremental × cost
    """

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        cycles = (
            pl.when(pl.col(Cols.seconds) > pl.col(Cols.initial))
            .then((pl.col(Cols.seconds) / pl.col(Cols.incremental)).ceil())
            .otherwise(pl.col(Cols.initial))
        )

        return df.with_columns(
            (cycles * pl.col(Cols.incremental) * pl.col(Cols.cost))
            .cast(pl.Float64)
            .alias(Cols.credits)
        )
