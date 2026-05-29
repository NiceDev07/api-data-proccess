import asyncio

import polars as pl

from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig
from modules.process.domain.utils import normalize_columns, rename_unnamed_columns


class XlsxReader(IFileReader):
    def __init__(self, *, preview_mode: bool = False) -> None:
        # preview_mode=True: castea todas las columnas a string y preserva
        # los encabezados originales — solo renombra los vacíos a column_N.
        self.preview_mode = preview_mode

    async def read(self, config: BaseFileConfig) -> pl.LazyFrame:
        if config is None:
            raise ValueError("XlsxReader.read() recibió config=None")

        file_path = config.folder

        def _load() -> pl.LazyFrame:
            kwargs = {"has_header": config.useHeaders}
            # preview_mode: infer_schema_length=0 hace que Polars lea todas las
            # columnas como String desde el inicio, sin pasar por Float64 — así
            # los enteros de Excel (que internamente son Float) se ven sin ".0".
            if self.preview_mode:
                # infer_schema_length=0 fuerza String en todas las columnas desde el inicio;
                # evita que enteros de Excel (internamente Float) aparezcan con ".0".
                kwargs["infer_schema_length"] = 0
            lf = pl.read_excel(file_path, **kwargs).lazy()
            # XLSX siempre se carga completo (limitación del formato); n_rows se aplica después.
            if config.n_rows is not None:
                lf = lf.limit(config.n_rows)
            return lf

        try:
            lf = await asyncio.to_thread(_load)
            cols = list(lf.collect_schema())
            rename = rename_unnamed_columns(cols) if self.preview_mode else normalize_columns(cols)
            if rename:
                lf = lf.rename(rename)
            # Elimina filas donde todas las columnas son nulas (filas vacías de Excel)
            return lf.filter(pl.any_horizontal(pl.all().is_not_null()))
        except FileNotFoundError:
            raise FileNotFoundError("FILE_NOT_FOUND: Campaign file not found.")
        except Exception:
            raise ValueError("FILE_READ_ERROR: Error reading the campaign file.")
