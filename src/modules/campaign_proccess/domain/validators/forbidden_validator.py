from typing import Iterable
from ..utils.clean_content import normalize_text

class ForbiddenWordsValidator:
    def __init__(self, forbidden_words: Iterable[str]):
        """
        forbidden_words: Palabras prohibidas ya limpias y en minúsculas.
        """
        self.forbidden_words = set(normalize_text(w) for w in forbidden_words if w)
        self._error_message = None

    def validate(self, clean_message: str) -> bool:
        for word in self.forbidden_words:
            if word in clean_message:
                self._error_message = f"El mensaje contiene una palabra no permitida: '{word}'"
                return False
            
        return True

    def error_message(self) -> str:
        return self._error_message or "El mensaje contiene palabras no autorizadas."