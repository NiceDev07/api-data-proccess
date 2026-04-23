import asyncio
import os

import polars as pl

from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig


class CsvReader(IFileReader):
    async def read(self, config: BaseFileConfig) -> pl.DataFrame:
        if config is None:
            raise ValueError("CsvReader.read() recibió config=None")

        file_path = os.path.join(config.folder)

        try:
            return await asyncio.to_thread(
                pl.read_csv,
                file_path,
                separator=config.delimiter,
                encoding="utf8",
                has_header=config.useHeaders,
                infer_schema_length=1000,
                ignore_errors=False,
            )
        except FileNotFoundError:
            raise FileNotFoundError(f"No se encontró el archivo CSV en: {file_path}")
        except UnicodeDecodeError:
            raise ValueError(f"Error de codificación al leer el archivo CSV: {file_path}")
        except Exception as e:
            raise ValueError(f"Error al leer CSV '{file_path}': {str(e)}")