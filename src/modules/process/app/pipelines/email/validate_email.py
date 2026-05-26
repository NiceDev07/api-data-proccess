import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason

_EMAIL_RE = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"


class ValidateEmail(IPipeline):
    # Invalida registros con formato de email incorrecto; respeta exclusiones anteriores
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        is_valid = pl.col(Cols.email).str.contains(_EMAIL_RE)
        to_invalidate = pl.col(Cols.is_ok) & ~is_valid

        return df.with_columns(
            pl.when(to_invalidate).then(pl.lit(False)).otherwise(pl.col(Cols.is_ok))
            .alias(Cols.is_ok),
            pl.when(to_invalidate)
            .then(pl.lit(ExclusionReason.INVALID_EMAIL))
            .otherwise(pl.col(Cols.error_code))
            .alias(Cols.error_code),
        )
