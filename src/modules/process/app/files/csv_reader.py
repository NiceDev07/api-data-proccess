import asyncio

import polars as pl

from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig
from modules.process.domain.utils import normalize_columns


class CsvReader(IFileReader):
    async def read(self, config: BaseFileConfig) -> pl.LazyFrame:
        if config is None:
            raise ValueError("CsvReader.read() recibió config=None")

        file_path = config.folder

        def _load() -> pl.LazyFrame:
            # read_csv + .lazy(): carga el archivo una vez y envuelve en plan lazy
            # para que toda la cadena de pasos se materialice una sola vez al final.
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
            lf = lf.rename(normalize_columns(list(lf.collect_schema())))
            # Elimina filas donde todas las columnas son nulas (líneas en blanco del CSV)
            return lf.filter(pl.any_horizontal(pl.all().is_not_null()))
        except FileNotFoundError:
            raise FileNotFoundError("FILE_NOT_FOUND: Campaign file not found.")
        except UnicodeDecodeError:
            raise ValueError("FILE_READ_ERROR: Encoding error while reading the campaign file.")
        except Exception:
            raise ValueError("FILE_READ_ERROR: Error reading the campaign file.")