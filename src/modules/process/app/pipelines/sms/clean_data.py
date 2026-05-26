import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.interfaces.normalizer import INormalizer
from logging_config import get_logger

logger = get_logger(__name__)


class CleanData(IPipeline):
    def __init__(self, normalizer: INormalizer):
        self.normalizer = normalizer

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        c = ctx.configFile.nameColumnDemographic
        if c not in df.columns:
            raise ValueError("COLUMN_NOT_FOUND: The demographic column was not found in the uploaded file.")

        total_antes = df.height

        # Usamos el mayor entre fijo y móvil como umbral mínimo de dígitos aceptado
        min_digits = max(ctx.rulesCountry.numberDigitsFixed, ctx.rulesCountry.numberDigitsMobile)

        result = (
            df.with_columns(self.normalizer.normalize(c))
              .filter(
                  pl.col(c).is_not_null() &
                  (pl.col(c).str.len_chars() >= min_digits)
              )
              .with_columns(pl.col(c).cast(pl.Int64, strict=False).alias(c))
        )

        logger.debug(
            "CleanData SMS | antes: %d | después de normalización y filtro mínimo %d dígitos: %d",
            total_antes, min_digits, result.height,
        )
        return result
