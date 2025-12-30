from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig
import polars as pl
import os

class XlsxReader(IFileReader):
    async def read(self, config: BaseFileConfig) -> pl.DataFrame:
        if config is None:
            raise ValueError("XlsxReader.read() recibió config=None")

        file_path = os.path.join(config.folder)

        try:
            # Polars lee XLSX vía calamine (rápido, sin encoding issues)
            return pl.read_excel(
                file_path
            )

        except FileNotFoundError:
            raise FileNotFoundError(f"No se encontró el archivo XLSX en: {file_path}")

        except Exception as e:
            raise ValueError(f"Error al leer XLSX '{file_path}': {str(e)}")