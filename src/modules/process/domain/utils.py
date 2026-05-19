import unicodedata


def normalize_col_name(name: str) -> str:
    """Sin acentos, sin ñ, minúsculas, sin espacios."""
    nfd = unicodedata.normalize("NFD", name)
    without_accents = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return "".join(c for c in without_accents.lower() if not c.isspace())
