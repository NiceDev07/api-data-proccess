from ..entities.tariff import Tariff
from abc import ABC, abstractmethod

class ForbiddenWordsRepositoryInterface(ABC):
    @abstractmethod
    def get_word_not_allowed(self, user_id: int) -> list[str]:
        """Check if a word is not allowed in a specific country."""
        pass
        