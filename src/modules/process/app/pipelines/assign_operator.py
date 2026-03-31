import polars as pl
import numpy as np
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols

class AssignOperator(IPipeline):
    default_operator = "N/A"

    def __init__(self,
        numeration_service,
    ):
        self.numeration_service = numeration_service

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        phone_column = ctx.configFile.nameColumnDemographic
        starts, ends, operators = await self.numeration_service.get_ranges(ctx.rulesCountry.idCountry)
        df = df.with_columns(pl.col(phone_column).cast(pl.Int64))

        # Obtener los números como array NumPy
        numbers = df[phone_column].to_numpy()

        # Buscar índice del rango en que caen
        idxs = np.searchsorted(starts, numbers, side="right") - 1
        valid = (idxs >= 0) & (numbers <= ends[idxs])
        assigned_ops = np.where(valid, operators[idxs], self.default_operator)

        from modules.process.domain.constants.reasons import ExclusionReason

        has_operator = pl.Series(valid)
        return df.with_columns(
            pl.Series(Cols.number_operator, assigned_ops),
            pl.when(~has_operator)
            .then(pl.lit(False))
            .otherwise(pl.col(Cols.is_ok))
            .alias(Cols.is_ok),
            pl.when(~has_operator)
            .then(pl.lit(ExclusionReason.NO_OPERATOR))
            .otherwise(pl.col(Cols.error_code))
            .alias(Cols.error_code),
        )