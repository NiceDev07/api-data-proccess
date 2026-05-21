import asyncio
import io
import os

import polars as pl

from logging_config import get_logger

logger = get_logger(__name__)

# Encodings que se prueban en orden — latin1 siempre funciona como último recurso
_ENCODINGS = ("utf_8_sig", "cp1252", "latin1")

# Máximo de filas y columnas que se leen para el preview
_MAX_ROWS = 6
_MAX_COLS = 10


def _detect_encoding(path: str) -> str:
    # Intenta abrir el archivo con cada encoding; retorna el primero que no falle.
    # latin1 es el último y siempre tiene éxito (mapea los 256 valores de byte).
    for enc in _ENCODINGS:
        try:
            with open(path, encoding=enc) as f:
                f.read(4096)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return _ENCODINGS[-1]  # fallback explícito — solo alcanzable si _ENCODINGS está vacío


def _detect_delimiter(path: str, encoding: str) -> str:
    # Lee 64 KB y cuenta comas, puntos y coma y tabs fuera de comillas dobles
    try:
        with open(path, encoding=encoding, errors="replace") as f:
            sample = f.read(65536)
    except Exception:
        return ";"

    commas = 0
    semicolons = 0
    tabs = 0
    in_quotes = False

    for ch in sample:
        if ch == '"':
            in_quotes = not in_quotes
        elif not in_quotes:
            if ch == ',':
                commas += 1
            elif ch == ';':
                semicolons += 1
            elif ch == '\t':
                tabs += 1

    # Retorna el delimitador más frecuente; en caso de empate gana el punto y coma
    counts = {',': commas, ';': semicolons, '\t': tabs}
    return max(counts, key=lambda k: counts[k])


def _read_csv(file_path: str) -> list[str]:
    enc = _detect_encoding(file_path)
    sep = _detect_delimiter(file_path, enc)

    # Polars solo soporta UTF-8 — leemos solo las primeras líneas necesarias con Python
    # (evita cargar un archivo de cientos de miles de filas completo en memoria)
    lines = []
    with open(file_path, encoding=enc, errors="replace") as f:
        for _ in range(_MAX_ROWS):
            line = f.readline()
            if not line:
                break
            lines.append(line)

    content = "".join(lines)

    df = (
        pl.read_csv(
            io.BytesIO(content.encode("utf-8")),
            n_rows=_MAX_ROWS,
            has_header=False,
            separator=sep,
            infer_schema=False,
        )
    )
    df = df.select(df.columns[:_MAX_COLS]).fill_null("")

    # Serializa cada fila como un string plano usando el delimitador detectado
    return [sep.join(map(str, row)) for row in df.rows()]


def _read_xlsx(file_path: str) -> list[str]:
    df = pl.read_excel(file_path, read_options={"n_rows": _MAX_ROWS})
    df = df.select(df.columns[:_MAX_COLS]).fill_null("")


    # XLSX no tiene delimitador nativo — usamos coma para serializar
    return [",".join(map(str, row)) for row in df.rows()]


async def get_first_rows(folder: str, file: str, base_dir: str) -> dict:
    # Construye y resuelve la ruta real para prevenir path traversal (../, symlinks, etc.)
    file_path = os.path.realpath(os.path.join(folder, file))
    allowed   = os.path.realpath(base_dir)

    if not file_path.startswith(allowed + os.sep) and file_path != allowed:
        logger.warning("Path traversal attempt blocked: %s", file_path)
        raise PermissionError("ACCESS_DENIED: Path is outside the authorized directory.")

    if not os.path.isfile(file_path):
        logger.warning("File not found for preview: %s", file_path)
        raise FileNotFoundError("FILE_NOT_FOUND: The file was not found at the specified path.")

    ext = file.rsplit(".", 1)[-1].lower()
    reader = _read_csv if ext == "csv" else _read_xlsx

    rows = await asyncio.to_thread(reader, file_path)
    logger.info("Preview generado | archivo: %s | filas: %d", file, len(rows))
    return {"success": True, "data": rows}
