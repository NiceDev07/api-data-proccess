import polars as pl
from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig


# Lector nulo — se usa cuando no se envía archivo de exclusiones
class NullReader(IFileReader):
    async def read(self, config: BaseFileConfig) -> pl.LazyFrame:
        return pl.DataFrame().lazy()