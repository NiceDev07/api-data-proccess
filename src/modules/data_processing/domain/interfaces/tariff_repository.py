from abc import ABC, abstractmethod
from ..value_objects.tel_cost_info import TelcoCostInfo

class ICostRepository(ABC):
    @abstractmethod
    def get_prefix_cost_pairs(self, country_id: int, tariff_id: int, service: str) -> TelcoCostInfo:
        pass