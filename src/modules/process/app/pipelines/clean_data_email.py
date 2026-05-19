import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.interfaces.normalizer import INormalizer
from modules.process.domain.constants.cols import Cols


class CleanDataEmail(IPipeline):
    """Normaliza la columna de email y elimina registros vacíos/nulos."""

    def __init__(self, normalizer: INormalizer):
        self.normalizer = normalizer

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        c = ctx.configFile.nameColumnDemographic
        schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
        if c not in schema:
            raise ValueError(f"COLUMN_NOT_FOUND: The column '{c}' does not exist in the file.")
        return (
            df.with_columns(self.normalizer.normalize(c))
              .filter(pl.col(c).is_not_null())
              .rename({c: Cols.email})
              .with_columns(pl.col(Cols.email).alias(c))
        )
