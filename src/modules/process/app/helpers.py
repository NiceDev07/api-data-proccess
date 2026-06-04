from typing import Any, Dict

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


def build_no_valid_records_response(
    df: pl.DataFrame,
    summary_dump: Dict[str, Any],
    service: str,
    payload: DataProcessingDTO,
    total_excluded: int,
) -> Dict[str, Any]:
    # Respuesta común cuando todos los registros quedan excluidos en SMS, Email o CB.
    # Agrupa los códigos de error para que el frontend sepa por qué fallaron y
    # evita que el cliente envíe a BD una campaña vacía sin darse cuenta.
    reasons = (
        df.filter(pl.col(Cols.error_code).is_not_null())
        .group_by(Cols.error_code)
        .agg(pl.len().alias("affected"))
        .to_dicts()
    )
    logger.error(
        "%s completado | válidos: 0 | excluidos: %d | campaña: %s",
        service, total_excluded, payload.campaignId,
    )
    return {
        "success": False,
        "error": {
            "code": "NO_VALID_RECORDS",
            "reasons": [{"code": r[Cols.error_code], "affected": r["affected"]} for r in reasons],
        },
        **summary_dump,
    }
