from ..entities.tariff import Tariff
from typing import Optional
from abc import ABC, abstractmethod

class ITariffRepository(ABC):
    @abstractmethod
    def get_tariff(self, country_id: int, tariff_id: int, service: str) -> Optional[Tariff]:
        pass
        