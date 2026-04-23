from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig
import polars as pl


# NullReader es un lector de archivos que no lee nada y devuelve un DataFrame vacío.
# APLICA PARA CUANDO EL USUARIO NO SUBE NINGUN ARCHIVO EN LA LISTA DE EXCLUSIONES
class NullReader(IFileReader):
    async def read(self, config: BaseFileConfig) -> pl.LazyFrame:
        return pl.DataFrame().lazy()