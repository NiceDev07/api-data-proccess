import asyncio
import os

import polars as pl

from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig
from modules.process.domain.utils import normalize_col_name


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
            lf = await asyncio.to_thread(_load)
            return lf.rename({c: normalize_col_name(c) for c in lf.collect_schema()})
        except FileNotFoundError:
            raise FileNotFoundError("FILE_NOT_FOUND: Campaign file not found.")
        except UnicodeDecodeError:
            raise ValueError("FILE_READ_ERROR: Encoding error while reading the campaign file.")
        except Exception as e:
            raise ValueError("FILE_READ_ERROR: Error reading the campaign file.")