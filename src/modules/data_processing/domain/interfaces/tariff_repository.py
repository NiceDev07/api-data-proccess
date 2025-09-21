from abc import ABC, abstractmethod

class ICostRepository(ABC):
    @abstractmethod
    async def get_tariff_costs(self, country_id: int, tariff_id: int, service: str) -> list[tuple[str, float]]:
        pass