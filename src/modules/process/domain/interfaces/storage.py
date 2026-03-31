from abc import ABC, abstractmethod
import polars as pl


class IStorage(ABC):
    @abstractmethod
    async def save(self, df: pl.DataFrame, filename: str) -> str:
        """Persiste el DataFrame y retorna la ruta o URL resultante."""
        pass
