import asyncio

import numpy as np
import polars as pl

from modules.process.domain.constants.cols import Cols
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.app.pipelines.sms._operator_common import DEFAULT_OPERATOR, mark_no_operator


class AssignOperator(IPipeline):
    """MASIVO: asigna operador por rangos de numeración (búsqueda vectorizada sobre
    millones de filas). No consulta portabilidad — eso es del flujo unitario (SP)."""

    default_operator = DEFAULT_OPERATOR

    def __init__(self, numeration_service):
        self.numeration_service = numeration_service

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        phone_column = ctx.configFile.nameColumnDemographic
        starts, ends, operators = await self.numeration_service.get_ranges(ctx.rulesCountry.idCountry)
        df = df.with_columns(pl.col(phone_column).cast(pl.Int64))

        numbers = df[phone_column].to_numpy()

        # NumPy binary search es CPU-bound — se ejecuta en thread pool para no bloquear el event loop.
        def _compute():
            idxs = np.searchsorted(starts, numbers, side="right") - 1
            valid = (idxs >= 0) & (numbers <= ends[idxs])
            assigned_ops = np.where(valid, operators[idxs], self.default_operator)
            return valid, assigned_ops

        valid, assigned_ops = await asyncio.to_thread(_compute)

        # dtype=pl.String evita que Polars infiera Object cuando el array de numpy llega
        # con dtype=object (desde NumerationService) o está vacío (todos filtrados antes).
        # Object rompe la escritura a Parquet en SaveResults.
        df = df.with_columns(pl.Series(Cols.number_operator, assigned_ops, dtype=pl.String))
        return mark_no_operator(df, pl.Series(valid))
