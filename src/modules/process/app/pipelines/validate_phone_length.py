import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason


class ValidatePhoneLength(IPipeline):
    """Marca como is_ok=False los registros cuyo número no tenga exactamente
    numberDigitsMobile o numberDigitsFixed dígitos.

    Solo afecta registros que aún están activos (is_ok=True) para no
    sobreescribir exclusiones anteriores.
    """

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        c = ctx.configFile.nameColumnDemographic
        rules = ctx.rulesCountry

        digit_count = pl.col(c).cast(pl.Utf8).str.len_chars()
        is_valid_length = (
            (digit_count == rules.numberDigitsMobile) |
            (digit_count == rules.numberDigitsFixed)
        )
        # Solo invalida registros que estaban ok; respeta exclusiones previas
        to_invalidate = pl.col(Cols.is_ok) & ~is_valid_length

        return df.with_columns(
            pl.when(to_invalidate).then(pl.lit(False)).otherwise(pl.col(Cols.is_ok))
            .alias(Cols.is_ok),
            pl.when(to_invalidate).then(pl.lit(ExclusionReason.INVALID_NUMBER_LENGTH)).otherwise(pl.col(Cols.error_code))
            .alias(Cols.error_code),
        )
