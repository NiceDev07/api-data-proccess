import re
import unicodedata
import polars as pl

_TAG_RE      = re.compile(r"\{(\w+(?:-\d+)?)\}")   # captura el nombre del tag
_TAG_SPLIT   = re.compile(r"\{\w+(?:-\d+)?\}")      # sin captura, para split limpio


def _normalize_tag(name: str) -> str:
    """Normaliza el nombre del tag igual que normalize_col_name: sin acentos, minúsculas, sin espacios."""
    nfd = unicodedata.normalize("NFD", name)
    without_accents = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return "".join(c for c in without_accents.lower() if not c.isspace())


def build_template_expr(
    template_str: str,
    col_alias: str,
    available_columns: list[str] | None = None,
    label_map: dict[str, str] | None = None,
) -> pl.Expr:
    """Returns a Polars Expr that replaces {tag} placeholders with column values.

    Los tags se normalizan (minúsculas, sin acentos) antes de buscarlos en las columnas,
    por lo que {Telefono}, {telefono} y {TELEFONO} resuelven a la misma columna.

    If label_map is provided, wraps each column value with the TTS type prefix:
    {nombre} with type "N" → {N:Carlos} in the final message.
    Tags with type "undefined" or absent from label_map are left as-is.
    Tags not present in available_columns are replaced with empty string.
    """
    parts = _TAG_SPLIT.split(template_str)
    tags  = _TAG_RE.findall(template_str)
    if not tags:
        return pl.lit(template_str).alias(col_alias)

    exprs: list[pl.Expr] = []
    for i, literal in enumerate(parts):
        if literal:
            exprs.append(pl.lit(literal))
        if i < len(tags):
            tag            = tags[i]
            normalized_tag = _normalize_tag(tag)
            if available_columns is not None and normalized_tag not in available_columns:
                exprs.append(pl.lit(""))
            else:
                # fill_null("") evita que una celda vacía propague null a toda la
                # fila por concat_str — el mensaje quedaría sin texto y el
                # proveedor descartaría el envío.
                col_expr = pl.col(normalized_tag).cast(pl.Utf8).fill_null("")
                tipo = (label_map or {}).get(tag, "") or (label_map or {}).get(normalized_tag, "")
                if tipo and tipo != "undefined":
                    col_expr = pl.concat_str([pl.lit(f"{{{tipo}:"), col_expr, pl.lit("}")])
                exprs.append(col_expr)

    return pl.concat_str(exprs, ignore_nulls=False).alias(col_alias)
