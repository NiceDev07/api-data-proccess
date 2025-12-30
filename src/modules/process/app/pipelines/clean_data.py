import polars as pl
from modules.data_processing.domain.interfaces.pipelines import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO

class CleanData(IPipeline):
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        c = ctx.configFile.nameColumnDemographic
        MIN_DIGITS = max(ctx.rulesCountry.numberDigitsFixed, ctx.rulesCountry.numberDigitsMobile)

        cleaned = (
            pl.col(c)
            .cast(pl.Utf8)
            .str.strip_chars()
            # 1) elimina TODOS los espacios (incluye tabs, etc.)
            .str.replace_all(r"\s+", "")
            # 2) ignora decimales: toma solo lo anterior al primer punto
            .str.split_exact(".", 2).struct.field("field_0")
            # 3) deja solo dígitos (regex simple y rápida)
            .str.replace_all(r"\D+", "")
            # 4) "" -> null (para poder filtrar)
            .replace("", None)
            .alias(c)
        )

        return (
            df.with_columns(cleaned)
              .filter(
                  pl.col(c).is_not_null() &
                  (pl.col(c).str.len_chars() >= MIN_DIGITS)
              )
              .with_columns(pl.col(c).cast(pl.Int64, strict=False).alias(c))
        )
