import polars as pl
from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig


class InlineReader(IFileReader):
    """Lector 'inline' para el flujo unitario: arma el DataFrame desde una lista de
    números en memoria (sin leer archivo). Reutiliza el mismo pipeline que el masivo
    — solo cambia el origen de los datos. Mismo patrón que NullReader."""

    def __init__(self, numbers: list):
        self._numbers = numbers

    async def read(self, config: BaseFileConfig) -> pl.LazyFrame:
        return pl.DataFrame({config.nameColumnDemographic: self._numbers}).lazy()
