import re

def extract_tags_from_content(content: str = "") -> set[str]:
    """Extrae todos los patrones {tag} de un string"""
    if content is None:
        return set()
    return set(re.findall(r"\{(\w+(?:-\d+)?)\}", content))


def extract_tags_with_repeats(content: str = "") -> list[str]:
    """Extrae todas las tags en orden, incluyendo repeticiones (para pl.format)"""
    if not content:
        return []
    return re.findall(r"{(\w+(?:-\d+)?)\}", content)