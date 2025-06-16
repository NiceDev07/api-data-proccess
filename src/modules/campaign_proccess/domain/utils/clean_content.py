import re
import unicodedata

def clean_content(message: str) -> str:
    """
    Limpia el mensaje removiendo:
    - Etiquetas entre llaves: {name}, {tag}, etc.
    - Emojis y caracteres no ASCII.
    - Caracteres separadores como ., -, _, espacios.
    - Tildes y diacríticos.
    Devuelve el mensaje en minúsculas y sin ruido.
    """
    if not message:
        return ""

    # Eliminar etiquetas como {name}, {saldo}, etc.
    message = re.sub(r"\{.*?\}", "", message)

    # Normalizar a ASCII: elimina emojis y caracteres raros
    message = unicodedata.normalize("NFKD", message)
    message = message.encode("ascii", "ignore").decode()

    # Eliminar separadores comunes usados para evadir validaciones
    message = re.sub(r"[.\-_\s]", "", message)

    # Quitar tildes (aunque ya con NFKD se hace)
    message = re.sub(r"[áàäâ]", "a", message)
    message = re.sub(r"[éèëê]", "e", message)
    message = re.sub(r"[íìïî]", "i", message)
    message = re.sub(r"[óòöô]", "o", message)
    message = re.sub(r"[úùüû]", "u", message)

    return message.lower()


def normalize_text(text: str) -> str:
    """
    - Quita tildes y signos
    - Convierte a minúsculas
    - Elimina espacios, guiones, puntos, underscores
    - Quita emojis y normaliza a ascii
    """
    # Quitar {tags}
    text = re.sub(r"\{.*?\}", "", text)

    # Convertir a ascii (quita emojis y tildes combinadas)
    text = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("utf-8")

    # Quitar puntuación que sirve para evadir detección
    text = re.sub(r"[.\-_\s]", "", text)

    # Pasar a minúsculas
    return text.lower()