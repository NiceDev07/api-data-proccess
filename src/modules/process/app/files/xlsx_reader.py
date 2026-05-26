import asyncio

import polars as pl

from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig
from modules.process.domain.utils import normalize_columns


class XlsxReader(IFileReader):
    async def read(self, config: BaseFileConfig) -> pl.LazyFrame:
        if config is None:
            raise ValueError("XlsxReader.read() recibió config=None")

        file_path = config.folder

        def _load() -> pl.LazyFrame:
            lf = pl.read_excel(
                file_path,
                has_header=config.useHeaders,
            ).lazy()
            # read_excel no soporta n_rows nativo — aplicamos limit() sobre el LazyFrame.
            # Para XLSX el archivo se carga completo de todas formas (limitación del formato).
            if config.n_rows is not None:
                lf = lf.limit(config.n_rows)
            return lf

        try:
            lf = await asyncio.to_thread(_load)
            lf = lf.rename(normalize_columns(list(lf.collect_schema())))
            # Elimina filas donde todas las columnas son nulas (filas vacías de Excel)
            return lf.filter(pl.any_horizontal(pl.all().is_not_null()))
        except FileNotFoundError:
            raise FileNotFoundError("FILE_NOT_FOUND: Campaign file not found.")
        except Exception:
            raise ValueError("FILE_READ_ERROR: Error reading the campaign file.")