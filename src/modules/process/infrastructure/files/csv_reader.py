from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig
import polars as pl
import os

class CsvReader(IFileReader):
    async def read(self, config: BaseFileConfig) -> pl.DataFrame:
        """
        Lee un archivo CSV usando Polars basado en la configuración enviada en BaseFileConfig.
        """
        if config is None:
            raise ValueError("CsvReader.read() recibió config=None")

        # Construir la ruta completa del archivo
        file_path = os.path.join(config.folder)

        # Leer CSV con polars
        try:
            df = pl.read_csv(
                file_path,
                separator=config.delimiter,
                has_header=config.useHeaders,
                infer_schema_length=1000,   # más seguro para archivos medianos
                ignore_errors=False
            )
            return df

        except FileNotFoundError:
            raise FileNotFoundError(f"No se encontró el archivo CSV en: {file_path}")
        except UnicodeDecodeError:
            raise ValueError(f"Error de codificación al leer el archivo CSV: {file_path}")
        except Exception as e:
            raise ValueError(f"Error al leer CSV '{file_path}': {str(e)}")