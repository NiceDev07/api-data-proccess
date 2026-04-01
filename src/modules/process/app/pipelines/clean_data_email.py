import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.interfaces.normalizer import INormalizer


class CleanDataEmail(IPipeline):
    """Normaliza la columna de email y elimina registros vacíos/nulos."""

    def __init__(self, normalizer: INormalizer):
        self.normalizer = normalizer

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        c = ctx.configFile.nameColumnDemographic
        return (
            df.with_columns(self.normalizer.normalize(c))
              .filter(pl.col(c).is_not_null())
        )
