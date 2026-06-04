import asyncio
import io

import polars as pl

from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.domain.models.process_dto import BaseFileConfig
from modules.process.domain.utils import normalize_columns, rename_unnamed_columns

# Encodings probados en orden — pensado para archivos reales de Latinoamérica.
# latin1 es la red de seguridad final: acepta cualquier byte (0x00-0xFF),
# por lo que nunca lanza UnicodeDecodeError.
_ENCODINGS_TO_TRY = ("utf-8-sig", "utf-8", "cp1252", "latin1")


def _to_utf8(file_path: str) -> io.BytesIO | str:
    # UTF-8 sin BOM: devolvemos la ruta para que Polars lea directo de disco (cero copia).
    # UTF-16 con BOM: lo decodificamos aparte porque ninguno de los encodings de la lista lo cubre.
    # Otros: probamos en orden y devolvemos BytesIO ya convertido a UTF-8.
    with open(file_path, "rb") as f:
        raw = f.read()

    # Mac clásico: solo "\r" como salto de línea. Polars no lo detecta y
    # leería el archivo como una sola fila gigante — lo normalizamos a "\n".
    needs_eol_fix = b"\r" in raw and b"\n" not in raw

    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return io.BytesIO(raw.decode("utf-16").encode("utf-8"))

    if not needs_eol_fix and not raw.startswith(b"\xef\xbb\xbf"):
        try:
            raw.decode("utf-8")
            return file_path
        except UnicodeDecodeError:
            pass

    if needs_eol_fix:
        raw = raw.replace(b"\r", b"\n")

    for encoding in _ENCODINGS_TO_TRY:
        try:
            return io.BytesIO(raw.decode(encoding).encode("utf-8"))
        except UnicodeDecodeError:
            continue
    # Inalcanzable en la práctica: latin1 nunca falla.
    return io.BytesIO(raw)


class CsvReader(IFileReader):
    def __init__(self, *, preview_mode: bool = False) -> None:
        # preview_mode=True: no infiere tipos (todo como string) y preserva
        # los encabezados originales — solo renombra los vacíos a column_N.
        self.preview_mode = preview_mode

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
                ignore_errors=False,
            )
            if self.preview_mode:
                # Sin inferencia: todo llega como String para que el frontend vea
                # "5727242" en lugar de "5727242.0".
                kwargs["infer_schema"] = False
            else:
                # Inferir tipos con las primeras 1000 filas es suficiente y evita
                # leer el archivo entero solo para detectar el schema.
                kwargs["infer_schema_length"] = 1000
            if not is_bytes and config.n_rows is not None:
                kwargs["n_rows"] = config.n_rows

            def _read(extra: dict | None = None) -> pl.LazyFrame:
                if is_bytes:
                    source.seek(0)
                return pl.read_csv(source, **{**kwargs, **(extra or {})}).lazy()

            try:
                lf = _read()
            except Exception:
                try:
                    # Números con separadores regionales (ej. "2.655.000") rompen
                    # la inferencia Float64; reintentamos leyendo todo como string.
                    lf = _read({"infer_schema": False})
                except Exception:
                    try:
                        # CSV mal formado: filas con más columnas que el header.
                        # truncate_ragged_lines=True descarta los campos extra en lugar de fallar.
                        lf = _read({"infer_schema": False, "truncate_ragged_lines": True})
                    except Exception:
                        # Último recurso: comillas mal balanceadas o envolviendo la fila completa.
                        # quote_char=None desactiva el manejo de quotes; ignore_errors saltea filas
                        # que aún así no se puedan parsear.
                        lf = _read({
                            "infer_schema": False,
                            "truncate_ragged_lines": True,
                            "ignore_errors": True,
                            "quote_char": None,
                        })

            if is_bytes and config.n_rows is not None:
                lf = lf.head(config.n_rows)
            return lf

        try:
            lf = await asyncio.to_thread(_load)
            cols = list(lf.collect_schema())
            rename = rename_unnamed_columns(cols) if self.preview_mode else normalize_columns(cols)
            if rename:
                lf = lf.rename(rename)
            return lf.filter(pl.any_horizontal(pl.all().is_not_null()))
        except FileNotFoundError:
            raise FileNotFoundError("FILE_NOT_FOUND: Campaign file not found.")
        except UnicodeDecodeError:
            raise ValueError("FILE_READ_ERROR: Encoding error while reading the campaign file.")
        except Exception:
            raise ValueError("FILE_READ_ERROR: Error reading the campaign file.")
