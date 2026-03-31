from modules._common.domain.interfaces.cache import ICache
from modules.process.infrastructure.repositories.cost import ServiceKey


class CostService:
    KEY = "cost"
    TTL = 60 * 5

    def __init__(self, cost_repo, cache: ICache):
        self.cost_repo = cost_repo
        self.cache = cache

    def _key(self, country_id: int, tariff_id: int, service: str) -> str:
        return f"{self.KEY}:t{tariff_id}:c{country_id}:s{service}"

    async def get_costs(
        self, country_id: int, tariff_id: int, service: ServiceKey
    ) -> list[tuple[str, float, str]]:
        key = self._key(country_id, tariff_id, service)
        cached = await self.cache.get(key)
        if cached is not None:
            return cached

        costs = await self.cost_repo.get_tariff_costs(country_id, tariff_id, service)
        # self.cache.set(key, costs, self.TTL)
        return costs
