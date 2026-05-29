import asyncio
import io

import polars as pl
from charset_normalizer import from_bytes

from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig
from modules.process.domain.utils import normalize_columns

_DETECTION_SAMPLE = 256 * 1024
_RESIDUAL_BOM_CHARS = ("﻿", "þÿ", "ÿþ")
# cp1250 y cp1252 comparten bytes; en empate preferimos cp1252 (Latinoamérica).
_CHAOS_TIE_THRESHOLD = 0.01


def _resolve_encoding(sample: bytes) -> str | None:
    result = from_bytes(sample)
    best = result.best()
    if best is None:
        return None
    if best.encoding == "cp1252":
        return "cp1252"
    for c in result:
        if c.encoding == "cp1252" and abs(c.chaos - best.chaos) < _CHAOS_TIE_THRESHOLD:
            return "cp1252"
    return best.encoding


def _to_utf8(file_path: str) -> io.BytesIO | str:
    """
    Devuelve la ruta cuando es UTF-8 sin BOM (Polars lee directo de disco).
    En cualquier otro encoding devuelve un BytesIO ya convertido a UTF-8.
    """
    with open(file_path, "rb") as f:
        sample = f.read(_DETECTION_SAMPLE)

    has_utf8_bom = sample.startswith(b"\xef\xbb\xbf")
    detected = _resolve_encoding(sample)

    if not has_utf8_bom and detected in (None, "utf_8"):
        return file_path

    with open(file_path, "rb") as f:
        raw = f.read()

    encoding = "utf-8-sig" if has_utf8_bom else detected
    text = raw.decode(encoding, errors="replace")
    for residual in _RESIDUAL_BOM_CHARS:
        if text.startswith(residual):
            text = text[len(residual):]
            break
    return io.BytesIO(text.encode("utf-8"))


class CsvReader(IFileReader):
    async def read(self, config: BaseFileConfig) -> pl.LazyFrame:
        if config is None:
            raise ValueError("CsvReader.read() recibió config=None")

        file_path = config.folder

        def _load() -> pl.LazyFrame:
            source = _to_utf8(file_path)
            is_bytes = isinstance(source, io.BytesIO)

            kwargs: dict = dict(
                separator=config.delimiter,
                encoding="utf8-lossy",
                quote_char='"',
                has_header=config.useHeaders,
                infer_schema_length=1000,
                ignore_errors=False,
            )
            if not is_bytes and config.n_rows is not None:
                kwargs["n_rows"] = config.n_rows

            def _read(extra: dict | None = None) -> pl.LazyFrame:
                if is_bytes:
                    source.seek(0)
                return pl.read_csv(source, **{**kwargs, **(extra or {})}).lazy()

            try:
                lf = _read()
            except Exception:
                # Números con separadores regionales (ej. "2.655.000") rompen
                # la inferencia Float64; reintentamos leyendo todo como string.
                lf = _read({"infer_schema": False})

            if is_bytes and config.n_rows is not None:
                lf = lf.head(config.n_rows)
            return lf

        try:
            lf = await asyncio.to_thread(_load)
            lf = lf.rename(normalize_columns(list(lf.collect_schema())))
            return lf.filter(pl.any_horizontal(pl.all().is_not_null()))
        except FileNotFoundError:
            raise FileNotFoundError("FILE_NOT_FOUND: Campaign file not found.")
        except UnicodeDecodeError:
            raise ValueError("FILE_READ_ERROR: Encoding error while reading the campaign file.")
        except Exception:
            raise ValueError("FILE_READ_ERROR: Error reading the campaign file.")
