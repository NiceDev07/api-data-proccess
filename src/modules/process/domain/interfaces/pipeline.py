from abc import ABC, abstractmethod
import polars as pl

class IPipeline(ABC):
    @abstractmethod
    def execute(self, df: pl.DataFrame) -> pl.DataFrame:
        pass