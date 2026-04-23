import re
import polars as pl

_TAG_RE = re.compile(r"\{(\w+(?:-\d+)?)\}")


def build_template_expr(template_str: str, col_alias: str) -> pl.Expr:
    """Returns a Polars Expr that replaces {tag} placeholders with column values."""
    tags = _TAG_RE.findall(template_str)
    if not tags:
        return pl.lit(template_str).alias(col_alias)
    return pl.format(
        _TAG_RE.sub("{}", template_str),
        *[pl.col(t).cast(pl.Utf8) for t in tags],
    ).alias(col_alias)
