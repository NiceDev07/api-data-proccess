from abc import ABC, abstractmethod
from ..value_objects.tel_cost_info import TelcoCostInfo

class ICostRepository(ABC):
    @abstractmethod
    def get_tariff_cost_data(self, country_id: int, tariff_id: int, service: str) -> TelcoCostInfo:
        pass