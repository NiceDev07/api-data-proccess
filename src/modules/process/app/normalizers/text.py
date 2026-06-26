import re
import unicodedata

_TAG_RE = re.compile(r"\{[^}]*\}")
_SEPARATOR_RE = re.compile(r"""[.\-_\s/\\*|@#!?+=~:;,()\[\]{}&'"<>`^]""")

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


def normalize_for_filter(text: str) -> str:
    if not text:
        return ""
    text = _TAG_RE.sub("", text)
    text = text.translate(_ACCENT_MAP)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = text.translate(_LEET_MAP)
    text = _SEPARATOR_RE.sub("", text)
    return text.lower()
