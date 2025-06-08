from ..entities.tariff import Tariff
from typing import Optional

class TariffRepositoryInterface:
    def get_tariff(self, country_id: int, tariff_id: int, service: str) -> Optional[Tariff]:
        pass
        