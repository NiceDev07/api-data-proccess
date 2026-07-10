import polars as pl

from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason

# Operador por defecto cuando el número no cae en ningún rango / no resuelve.
DEFAULT_OPERATOR = "N/A"


def mark_no_operator(df: pl.DataFrame, resolved: pl.Series) -> pl.DataFrame:
    """Invalida (is_ok=False, error_code=NO_OPERATOR) los registros cuyo `resolved`
    es False, sin pisar un error_code previo (solo toca los que aún están OK).

    Compartido por AssignOperator (masivo, numeración vectorizada) y
    AssignOperatorRouting (unitario, SP) para no duplicar el bloque de invalidación.
    """
    to_invalidate = ~resolved & pl.col(Cols.is_ok)
    return df.with_columns(
        pl.when(to_invalidate)
        .then(pl.lit(False))
        .otherwise(pl.col(Cols.is_ok))
        .alias(Cols.is_ok),
        pl.when(to_invalidate)
        .then(pl.lit(ExclusionReason.NO_OPERATOR))
        .otherwise(pl.col(Cols.error_code))
        .alias(Cols.error_code),
    )
