import polars as pl
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.interfaces.normalizer import INormalizer

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