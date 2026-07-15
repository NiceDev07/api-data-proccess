import re
import unicodedata

_TAG_RE = re.compile(r"\{[^}]*\}")
_SEPARATOR_RE = re.compile(r"""[.\-_\s/\\*|@#!?+=~:;,()\[\]{}&'"<>`^]""")
# Igual que _SEPARATOR_RE pero sin \s — se usa en la limpieza final para NO
# borrar el espacio (que ahí ya cumplió su rol de límite entre palabras).
_SEPARATOR_NO_SPACE_RE = re.compile(r"""[.\-_/\\*|@#!?+=~:;,()\[\]{}&'"<>`^]""")
# Detecta evasión letra-por-letra ("n e q u i", "n-e-q-u-i"): 4+ caracteres
# sueltos separados uno a uno por el MISMO tipo de separador repetido. Solo
# colapsa estos separadores (no todos los del texto) para no fusionar dos
# palabras normales de una oración — ej. "la mora en" nunca debe convertirse
# en "lamoraen" (donde aparecería "amor" oculto en el empalme).
_LETTER_SPACED_RE = re.compile(
    r"""\w([.\-_\s/\\*|@#!?+=~:;,()\[\]{}&'"<>`^])(?:\w\1){2,}\w"""
)

_LEET_MAP = str.maketrans({
    '0': 'o',
    '1': 'i',
    '2': 'z',
    '3': 'e',
    '4': 'a',
    '5': 's',
    '6': 'b',
    '7': 't',
    '8': 'b',
    '9': 'g',
    '$': 's',
    '@': 'a',
    '!': 'i',
    '+': 't',
})

_ACCENT_MAP = str.maketrans({
    'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a',
    'Á': 'a', 'À': 'a', 'Ä': 'a', 'Â': 'a',
    'é': 'e', 'è': 'e', 'ë': 'e', 'ê': 'e',
    'É': 'e', 'È': 'e', 'Ë': 'e', 'Ê': 'e',
    'í': 'i', 'ì': 'i', 'ï': 'i', 'î': 'i',
    'Í': 'i', 'Ì': 'i', 'Ï': 'i', 'Î': 'i',
    'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o',
    'Ó': 'o', 'Ò': 'o', 'Ö': 'o', 'Ô': 'o',
    'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u',
    'Ú': 'u', 'Ù': 'u', 'Ü': 'u', 'Û': 'u',
    'ñ': 'n', 'Ñ': 'n',
})


def _collapse_letter_spacing(text: str) -> str:
    """Colapsa SOLO las corridas de letras sueltas separadas una a una (ej.
    'n e q u i' -> 'nequi'). El resto del texto conserva sus separadores
    hasta el paso final, que ya no borra espacios entre palabras normales."""
    return _LETTER_SPACED_RE.sub(lambda m: _SEPARATOR_RE.sub("", m.group(0)), text)


def normalize_for_filter(text: str) -> str:
    if not text:
        return ""
    text = _TAG_RE.sub("", text)
    text = text.translate(_ACCENT_MAP)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = text.translate(_LEET_MAP)
    text = _collapse_letter_spacing(text)
    # Los separadores restantes (puntuación) se borran igual que antes, pero
    # el espacio se preserva como límite real entre palabras — evita fusionar
    # dos palabras de una oración en una sola cadena (ver _LETTER_SPACED_RE).
    text = _SEPARATOR_NO_SPACE_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower()
