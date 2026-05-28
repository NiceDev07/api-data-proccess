import asyncio
import io

import polars as pl
from charset_normalizer import from_bytes

from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig
from modules.process.domain.utils import normalize_columns

_UTF16_BOMS = (b"\xff\xfe", b"\xfe\xff")


def _to_utf8(file_path: str) -> io.BytesIO | str:
    """
    Convierte el archivo a UTF-8 en memoria cuando su encoding no es UTF-8/ASCII.
    - UTF-16 (con BOM): decodifica con 'utf-16'.
    - UTF-8 BOM: quita el BOM para que no contamine el primer nombre de columna.
    - Otros (ISO-8859-6, KOI8-U, Windows-1256, OEM-720, …): charset-normalizer
      detecta el encoding y lo convierte; preserva el contenido correctamente.
    Para UTF-8 y ASCII puro devuelve la ruta original (sin copiar en memoria).
    """
    with open(file_path, "rb") as f:
        raw = f.read()

    if raw[:2] in _UTF16_BOMS:
        return io.BytesIO(raw.decode("utf-16").encode("utf-8"))

    if raw[:3] == b"\xef\xbb\xbf":
        return io.BytesIO(raw[3:])  # ya es UTF-8, solo eliminamos el BOM

    result = from_bytes(raw)
    best = result.best()
    if best is None:
        return file_path
    enc = best.encoding.lower().replace("-", "_")
    if enc in ("utf_8", "ascii"):
        return file_path
    return io.BytesIO(raw.decode(best.encoding, errors="replace").encode("utf-8"))


class CsvReader(IFileReader):
    async def read(self, config: BaseFileConfig) -> pl.LazyFrame:
        if config is None:
            raise ValueError("CsvReader.read() recibió config=None")

        file_path = config.folder

        def _load() -> pl.LazyFrame:
            source = _to_utf8(file_path)
            is_bytes = isinstance(source, io.BytesIO)

            kwargs = dict(
                separator=config.delimiter,
                encoding="utf8-lossy",
                quote_char='"',
                has_header=config.useHeaders,
                infer_schema_length=1000,
                ignore_errors=False,
            )
            # n_rows solo aplica sobre rutas de archivo, no sobre BytesIO
            if not is_bytes and config.n_rows is not None:
                kwargs["n_rows"] = config.n_rows  # type: ignore[assignment]

            try:
                if is_bytes:
                    source.seek(0)  # type: ignore[union-attr]
                lf = pl.read_csv(source, **kwargs).lazy()
            except Exception:
                # Fallback para archivos con números en formato regional
                # (ej. "2.655.000" con puntos como separadores de miles).
                if is_bytes:
                    source.seek(0)  # type: ignore[union-attr]
                lf = pl.read_csv(source, **{**kwargs, "infer_schema": False}).lazy()

            if is_bytes and config.n_rows is not None:
                lf = lf.head(config.n_rows)
            return lf

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