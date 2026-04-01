import polars as pl
from modules.process.domain.interfaces.normalizer import INormalizer


class EmailNormalizer(INormalizer):
    def normalize(self, col: str) -> pl.Expr:
        return (
            pl.col(col)
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_lowercase()
            .replace("", None)
            .alias(col)
        )
