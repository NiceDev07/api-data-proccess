import os

import polars as pl

from modules.process.app.files.factory import ReaderFileFactory
from modules.process.domain.models.process_dto import BaseFileConfig
from logging_config import get_logger

logger = get_logger(__name__)

# Máximo de filas y columnas que se leen para el preview
_MAX_ROWS = 6
_MAX_COLS = 10


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
    )

    # Leemos el archivo obteniendo un LazyFrame y luego materializamos
    # solo las primeras _MAX_ROWS filas para no cargar el archivo completo en memoria
    lf: pl.LazyFrame = await reader.read(config)
    df: pl.DataFrame = lf.limit(_MAX_ROWS).collect()

    # Limitamos a _MAX_COLS columnas para mantener la respuesta manejable
    df = df.select(df.columns[:_MAX_COLS])

    # Convertimos valores nulos a cadena vacía para que el frontend no reciba null
    df = df.fill_null("")

    # Construimos la respuesta estructurada separando encabezados de filas.
    # Cada fila es una lista de strings para facilitar el renderizado en tabla.
    headers: list[str] = df.columns
    rows: list[list[str]] = [
        [str(v) for v in row]
        for row in df.rows()
    ]

    logger.info("Preview generado | archivo: %s | filas: %d", file, len(rows))
    return {"success": True, "headers": headers, "rows": rows}
