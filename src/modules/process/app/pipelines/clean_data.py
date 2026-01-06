import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.interfaces.normalizer import INormalizer

class CleanData(IPipeline):
    def __init__(
        self,
        normalizer: INormalizer
    ):
        self.normalizer = normalizer

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        c = ctx.configFile.nameColumnDemographic
        MIN_DIGITS = max(ctx.rulesCountry.numberDigitsFixed, ctx.rulesCountry.numberDigitsMobile)
        cleaned = self.normalizer.normalize(c)

        return (
            df.with_columns(cleaned)
              .filter(
                  pl.col(c).is_not_null() &
                  (pl.col(c).str.len_chars() >= MIN_DIGITS)
              )
              .with_columns(pl.col(c).cast(pl.Int64, strict=False).alias(c))
        )
