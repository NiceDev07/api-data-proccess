import os

import polars as pl

from modules.process.app.files.factory import ReaderFileFactory
from modules.process.domain.models.process_dto import BaseFileConfig
from logging_config import get_logger

logger = get_logger(__name__)

# Máximo de filas que se leen para el preview.
# Se pasa como n_rows al reader para que pare a nivel de I/O (CSV)
# o se aplique como limit() sobre el LazyFrame (XLSX).
_MAX_ROWS = 6


async def get_first_rows(
    folder: str,
    file: str,
    delimiter: str,
    use_headers: bool,
    base_dir: str,
) -> dict:
    # ─────────────────────────────────────────────────────────────
    # Validación de seguridad: path traversal
    # Nos aseguramos de que el archivo esté dentro del directorio autorizado
    # antes de intentar leerlo, para evitar acceso a rutas arbitrarias del sistema.
    # ─────────────────────────────────────────────────────────────
    file_path = os.path.realpath(os.path.join(folder, file))
    allowed   = os.path.realpath(base_dir)

    if not file_path.startswith(allowed + os.sep) and file_path != allowed:
        logger.warning("Path traversal attempt blocked: %s", file_path)
        raise PermissionError("ACCESS_DENIED: Path is outside the authorized directory.")

    # Verificamos que el archivo exista en disco antes de intentar abrirlo
    if not os.path.isfile(file_path):
        logger.warning("File not found for preview: %s", file_path)
        raise FileNotFoundError("FILE_NOT_FOUND: The file was not found at the specified path.")

    # ─────────────────────────────────────────────────────────────
    # Obtención del reader correcto a través de la factory
    # ReaderFileFactory devuelve CsvReader, XlsxReader o NullReader
    # según la extensión del archivo, sin que aquí necesitemos saber
    # cuál es la implementación concreta.
    # ─────────────────────────────────────────────────────────────
    factory = ReaderFileFactory()
    reader  = factory.create(file)

    # Construimos la configuración mínima que necesitan los readers.
    # - folder: ruta completa al archivo (los readers usan config.folder como path)
    # - nameColumnDemographic: campo requerido por BaseFileConfig pero no usado
    #   en la lectura inicial; se pasa un placeholder para satisfacer el modelo.
    config = BaseFileConfig(
        folder=file_path,
        file=file,
        delimiter=delimiter or ";",
        useHeaders=use_headers,
        nameColumnDemographic="_",
        n_rows=_MAX_ROWS,
    )

    # El reader aplica n_rows internamente:
    # - CsvReader: pl.read_csv(n_rows=_MAX_ROWS) → para en I/O, no carga el archivo completo
    # - XlsxReader: lf.limit(_MAX_ROWS) → XLSX se carga completo igual (limitación del formato)
    lf: pl.LazyFrame = await reader.read(config)
    df: pl.DataFrame = lf.collect()

    # Convertimos valores nulos a cadena vacía para que el frontend no reciba null
    df = df.fill_null("")

    # Construimos la respuesta en el formato esperado por el frontend:
    # data[0]  = encabezados de columnas unidos por el delimitador
    # data[1:] = filas de datos, cada una como string unido por el delimitador
    sep: str = delimiter or ";"
    headers: list[str] = df.columns
    rows: list[str] = [sep.join(str(v) for v in row) for row in df.rows()]
    data: list[str] = [sep.join(headers)] + rows

    logger.info("Preview generado | archivo: %s | filas: %d", file, len(rows))
    return {"success": True, "data": data}
