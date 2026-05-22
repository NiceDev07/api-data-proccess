import polars as pl
from modules.process.domain.constants.cols import Cols
from modules.process.domain.models.process_dto import DataProcessingDTO
from logging_config import get_logger

logger = get_logger(__name__)

def attach_identifier(lf: pl.LazyFrame, payload: DataProcessingDTO) -> pl.LazyFrame:
    # Adjunta Cols.identifier al LazyFrame usando la columna indicada en el DTO.
    # Si la columna no existe o no está configurada, guarda cadena vacía.
    # Centraliza la lógica que antes estaba duplicada en cada procesador.
    col = payload.configFile.nameColumnIdentifier

    # Verificamos en el schema antes de leer la columna para evitar errores en runtime
    if col and col in lf.collect_schema():
        logger.debug("Identificador: se usará la columna '%s' como identificador de usuario", col)
        expr = (
            pl.col(col)
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_lowercase()
            .alias(Cols.identifier)
        )
    else:
        # Sin columna configurada o sin coincidencia → identificador vacío
        if not col:
            logger.debug("Identificador: no se configuró columna identificadora — se adjuntará cadena vacía")
        else:
            logger.debug("Identificador: la columna '%s' no existe en el archivo — se adjuntará cadena vacía", col)
        expr = pl.lit("").alias(Cols.identifier)

    return lf.with_columns(expr)
