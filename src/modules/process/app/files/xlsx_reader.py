import asyncio
import os

import polars as pl

from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig


class XlsxReader(IFileReader):
    async def read(self, config: BaseFileConfig) -> pl.LazyFrame:
        if config is None:
            raise ValueError("XlsxReader.read() recibió config=None")

        file_path = os.path.join(config.folder)

        def _load() -> pl.LazyFrame:
            return pl.read_excel(
                file_path,
                has_header=config.useHeaders,
            ).lazy()

        try:
            return await asyncio.to_thread(_load)
        except FileNotFoundError:
            raise FileNotFoundError("FILE_NOT_FOUND: Campaign file not found.")
        except Exception as e:
            raise ValueError("FILE_READ_ERROR: Error reading the campaign file.")