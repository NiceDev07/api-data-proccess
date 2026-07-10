import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.interfaces.normalizer import INormalizer
from modules.process.app.normalizers.number import to_national_expr
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

        # Umbral mínimo de dígitos aceptado: criterio único compartido (national_digits)
        min_digits = ctx.rulesCountry.national_digits

        # Si el número ya trae el prefijo de país (p. ej. 573001234567), se reduce a
        # su parte nacional para que se acepte igual que uno sin prefijo: así pasa la
        # validación de operador (rangos nacionales) y ConcatPrefix no duplica el 57.
        prefix = str(ctx.rulesCountry.codeCountry)
        full_len = len(prefix) + min_digits

        result = (
            df.with_columns(self.normalizer.normalize(c))
              .with_columns(to_national_expr(pl.col(c), prefix, full_len).alias(c))
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
