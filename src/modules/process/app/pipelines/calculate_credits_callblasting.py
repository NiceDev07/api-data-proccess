import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols


class CalculateCreditsCallBlasting(IPipeline):
    """Calcula créditos para call blasting.

    Fórmula:
        si seconds > initial → cycles = ⌈seconds / incremental⌉
        si no               → cycles = initial   (mínimo de facturación)
        cost_per_second = cost / 60  (cost viene por minuto desde DB)
        credits = cycles × incremental × cost_per_second
    """

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        cycles = (
            pl.when(pl.col(Cols.seconds) > pl.col(Cols.initial))
            .then((pl.col(Cols.seconds) / pl.col(Cols.incremental)).ceil())
            .otherwise(pl.col(Cols.initial))
        )

        cost_per_second = pl.col(Cols.cost) / 60

        return df.with_columns(
            (cycles * pl.col(Cols.incremental) * cost_per_second)
            .cast(pl.Float64)
            .round(3)
            .alias(Cols.credits)
        )
