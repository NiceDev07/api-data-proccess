import asyncio

import polars as pl

from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig
from modules.process.domain.utils import normalize_col_name


class XlsxReader(IFileReader):
    async def read(self, config: BaseFileConfig) -> pl.LazyFrame:
        if config is None:
            raise ValueError("XlsxReader.read() recibió config=None")

        file_path = config.folder

        def _load() -> pl.LazyFrame:
            return pl.read_excel(
                file_path,
                has_header=config.useHeaders,
            ).lazy()

        try:
            lf = await asyncio.to_thread(_load)
            lf = lf.rename({c: normalize_col_name(c) for c in lf.collect_schema()})
            # Elimina filas donde todas las columnas son nulas (filas vacías de Excel)
            return lf.filter(pl.any_horizontal(pl.all().is_not_null()))
        except FileNotFoundError:
            raise FileNotFoundError("FILE_NOT_FOUND: Campaign file not found.")
        except Exception:
            raise ValueError("FILE_READ_ERROR: Error reading the campaign file.")