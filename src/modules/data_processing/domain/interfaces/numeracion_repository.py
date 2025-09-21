from abc import ABC, abstractmethod
from typing import List, Tuple

class INumeracionRepository(ABC):
    @abstractmethod
    async def get_numeracion(self, country_id: int) -> List[Tuple[int, int, str]]:
        """Retrieve numeration data for a given country and service."""
        pass