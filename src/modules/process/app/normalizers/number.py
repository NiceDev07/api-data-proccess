import polars as pl
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.interfaces.normalizer import INormalizer


def to_national_expr(col: pl.Expr, prefix: str, full_len: int) -> pl.Expr:
    """Si el número YA trae el prefijo de país (es numérico, empieza por el prefijo
    y su largo es exactamente prefijo+nacional), devuelve solo su parte nacional; si
    no (número ya nacional, email, etc.), lo deja intacto. Idempotente: aplicarlo
    varias veces da el mismo resultado."""
    return (
        pl.when(
            (col.str.len_chars() == full_len)
            & col.str.starts_with(prefix)
            & col.str.contains(r"^\d+$")
        )
        .then(col.str.slice(len(prefix)))
        .otherwise(col)
    )


def to_national_str(value: str, prefix: str, full_len: int) -> str:
    """Versión escalar de `to_national_expr` para comparaciones puntuales."""
    if value.isdigit() and len(value) == full_len and value.startswith(prefix):
        return value[len(prefix):]
    return value

class NumberNormalizer(INormalizer):
    def normalize(self, col: str) -> pl.Expr:
        return (
            pl.col(col)
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
            .alias(col)
        )