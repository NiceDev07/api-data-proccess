from abc import ABC, abstractmethod
import polars as pl
from modules.process.domain.models.process_dto import DataProcessingDTO


class IRegulation(ABC):
    @abstractmethod
    def validate(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> None:
        """Lanza ValueError si la validación falla."""
        pass
