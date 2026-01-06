from typing import Protocol
import polars as pl

class INormalizer(Protocol):
    def normalize(self, col: str) -> pl.Expr: ...