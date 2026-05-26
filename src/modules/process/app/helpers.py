import polars as pl
from modules.process.domain.constants.cols import Cols
from modules.process.domain.models.process_dto import DataProcessingDTO
from logging_config import get_logger

logger = get_logger(__name__)

def attach_identifier(lf: pl.LazyFrame, payload: DataProcessingDTO) -> pl.LazyFrame:
    # Adjunta Cols.identifier al LazyFrame usando la columna indicada en el DTO.
    # Si la columna no existe o no está configurada, guarda cadena vacía.
    col = payload.configFile.nameColumnIdentifier

    if not col or col not in lf.collect_schema():
        return lf.with_columns(pl.lit("").alias(Cols.identifier))

    return lf.with_columns(
        pl.col(col).cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias(Cols.identifier)
    )
