import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason


class ValidatePhoneLength(IPipeline):
    # Invalida números que no tengan exactamente numberDigitsMobile o numberDigitsFixed dígitos;
    # respeta exclusiones anteriores (solo toca registros con is_ok=True)
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        c = ctx.configFile.nameColumnDemographic
        rules = ctx.rulesCountry

        digit_count = pl.col(c).cast(pl.Utf8).str.len_chars()
        is_valid_length = (
            (digit_count == rules.numberDigitsMobile) |
            (digit_count == rules.numberDigitsFixed)
        )
        to_invalidate = pl.col(Cols.is_ok) & ~is_valid_length

        return df.with_columns(
            pl.when(to_invalidate).then(pl.lit(False)).otherwise(pl.col(Cols.is_ok))
            .alias(Cols.is_ok),
            pl.when(to_invalidate).then(pl.lit(ExclusionReason.INVALID_NUMBER_LENGTH)).otherwise(pl.col(Cols.error_code))
            .alias(Cols.error_code),
        )
