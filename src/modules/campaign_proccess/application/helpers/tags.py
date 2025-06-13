import re

def extract_tags_from_content(content: str) -> set[str]:
    """Extrae todos los patrones {tag} de un string"""
    return set(re.findall(r"\{(\w+(?:-\d+)?)\}", content))
