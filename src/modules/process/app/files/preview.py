import os

import polars as pl

from modules.process.app.files.factory import ReaderFileFactory
from modules.process.domain.models.process_dto import BaseFileConfig
from logging_config import get_logger

logger = get_logger(__name__)

# Máximo de filas que se leen para el preview.
_MAX_ROWS = 6


async def get_first_rows(
    folder: str,
    file: str,
    delimiter: str,
    use_headers: bool,
    base_dir: str,
) -> dict:
    # Validación de seguridad: path traversal
    file_path = os.path.realpath(os.path.join(folder, file))
    allowed   = os.path.realpath(base_dir)

    if not file_path.startswith(allowed + os.sep) and file_path != allowed:
        logger.warning("Path traversal attempt blocked: %s", file_path)
        raise PermissionError("ACCESS_DENIED: Path is outside the authorized directory.")

    if not os.path.isfile(file_path):
        logger.warning("File not found for preview: %s", file_path)
        raise FileNotFoundError("FILE_NOT_FOUND: The file was not found at the specified path.")

    # Reader en modo preview: valores como string y encabezados originales.
    reader = ReaderFileFactory().create(file, preview=True)

    config = BaseFileConfig(
        folder=file_path,
        file=file,
        delimiter=delimiter or ";",
        useHeaders=use_headers,
        nameColumnDemographic="_",
        n_rows=_MAX_ROWS,
    )

    lf: pl.LazyFrame = await reader.read(config)
    df: pl.DataFrame = lf.collect().fill_null("")

    # config.delimiter ya incorpora el fallback a ";" — reutilizamos ese valor.
    sep = config.delimiter
    headers = df.columns
    rows = [sep.join(str(v) for v in row) for row in df.rows()]
    data = [sep.join(headers)] + rows

    logger.info("Preview generado | archivo: %s | filas: %d", file, len(rows))
    return {"success": True, "data": data}
