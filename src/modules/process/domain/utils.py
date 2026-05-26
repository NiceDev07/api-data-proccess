import re
import unicodedata

# Nombres que Polars asigna automáticamente a celdas de encabezado vacías.
# CSV:  primera vacía → ''  |  siguientes → _duplicated_N
# XLSX: todas las vacías    → __UNNAMED__N
_POLARS_AUTO_NAME_RE = re.compile(r"^(_duplicated_\d+|__UNNAMED__\d+)$")


def normalize_col_name(name: str) -> str:
    """Sin acentos, sin ñ, minúsculas, sin espacios."""
    nfd = unicodedata.normalize("NFD", name)
    without_accents = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return "".join(c for c in without_accents.lower() if not c.isspace())


def normalize_columns(columns: list[str]) -> dict[str, str]:
    # Construye el mapa de renombrado para todos los encabezados de un DataFrame.
    # Columnas sin nombre (vacías o auto-generadas por Polars) se asignan a
    # column_{posición} para coincidir con la convención del frontend.
    result: dict[str, str] = {}
    for i, col in enumerate(columns):
        normalized = normalize_col_name(col)
        if not normalized or _POLARS_AUTO_NAME_RE.match(col):
            result[col] = f"column_{i + 1}"
        else:
            result[col] = normalized
    return result
