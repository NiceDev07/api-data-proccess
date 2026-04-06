from modules._common.domain.interfaces.cache import ICache
from modules.process.infrastructure.repositories.cost import ServiceKey, CBServiceKey


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
        await self.cache.set(key, costs, self.TTL)
        return costs

    async def get_email_cost(
        self, country_id: int, tariff_id: int
    ) -> float | None:
        key = self._key(country_id, tariff_id, "email") + ":single"
        cached = await self.cache.get(key)
        if cached is not None:
            return cached

        cost = await self.cost_repo.get_email_cost(country_id, tariff_id)
        if cost is not None:
            await self.cache.set(key, cost, self.TTL)
        return cost

    async def get_costs_cb(
        self, country_id: int, tariff_id: int, service: CBServiceKey
    ) -> list[tuple[str, float, str, float, float]]:
        key = self._key(country_id, tariff_id, service) + ":cb"
        cached = await self.cache.get(key)
        if cached is not None:
            return cached

        costs = await self.cost_repo.get_tariff_costs_cb(country_id, tariff_id, service)
        await self.cache.set(key, costs, self.TTL)
        return costs
