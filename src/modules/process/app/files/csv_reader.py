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

        def _load() -> pl.LazyFrame:
            # read_csv carga el archivo una sola vez en memoria; .lazy() envuelve
            # el DataFrame para que toda la cadena de pasos corra como plan lazy
            # y se materialice una única vez en SaveResults.collect(streaming).
            kwargs = dict(
                separator=config.delimiter,
                encoding="utf8-lossy",
                quote_char='"',
                has_header=config.useHeaders,
                infer_schema_length=1000,
                ignore_errors=False,
            )
            try:
                return pl.read_csv(file_path, **kwargs).lazy()
            except Exception:
                # Fallback para archivos con números en formato regional
                # (ej. "2.655.000" con puntos como separadores de miles).
                return pl.read_csv(file_path, **{**kwargs, "infer_schema": False}).lazy()

        try:
            return await asyncio.to_thread(_load)
        except FileNotFoundError:
            raise FileNotFoundError(f"No se encontró el archivo CSV en: {file_path}")
        except UnicodeDecodeError:
            raise ValueError(f"Error de codificación al leer el archivo CSV: {file_path}")
        except Exception as e:
            raise ValueError(f"Error al leer CSV '{file_path}': {str(e)}")