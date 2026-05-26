import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.interfaces.normalizer import INormalizer
from modules.process.domain.constants.cols import Cols
from logging_config import get_logger

logger = get_logger(__name__)


class CleanDataEmail(IPipeline):
    def __init__(self, normalizer: INormalizer):
        self.normalizer = normalizer

    async def execute(self, df: pl.LazyFrame, ctx: DataProcessingDTO) -> pl.LazyFrame:
        c = ctx.configFile.nameColumnDemographic
        if c not in df.collect_schema():
            raise ValueError(f"COLUMN_NOT_FOUND: The column '{c}' does not exist in the file.")

        logger.debug("CleanDataEmail | normalizando emails en columna '%s'", c)
        return (
            df.with_columns(self.normalizer.normalize(c))
              .filter(pl.col(c).is_not_null())
              .rename({c: Cols.email})
              .with_columns(pl.col(Cols.email).alias(c))
        )
