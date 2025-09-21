from abc import ABC, abstractmethod
from typing import Any
import polars as pl

class IPipeline(ABC):
    @abstractmethod
    def execute(self, df: pl.DataFrame) -> pl.DataFrame:
        pass