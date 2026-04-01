from abc import ABC, abstractmethod
import polars as pl
from modules.process.domain.models.process_dto import DataProcessingDTO


class IRegulation(ABC):
    @abstractmethod
    def validate(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        """
        Valida los registros del DataFrame.
        Los registros que infrinjan la regulación se marcan con is_ok=False
        y su error_code correspondiente. Nunca lanza ValueError.
        """
        pass
