from modules.data_processing.domain.interfaces.tariff_repository import ICostRepository
from modules.data_processing.domain.interfaces.cache_interface import ICache

class CostService:
    KEY = "costs"
    TTL = 60*5
    def __init__(
        self,
        cost_repo: ICostRepository,
        cache: ICache
    ):
        self.cost_repo = cost_repo
        self.cache = cache

    def __get_key(self, country_id: int, tariff_id:int, service: str):
        return f"cost:t{tariff_id}:c{country_id}:s{service}"


    async def get_costs(self, country_id: int, tariff_id:int, service: str):
        key = self.__get_key(country_id, tariff_id, service)
        prefix_costs = await self.cache.get(key)

        if prefix_costs is None:
           costs = await self.cost_repo.get_tariff_costs(country_id, tariff_id, service)
           prefix_costs = sorted(costs, key=lambda x: len(x[0]), reverse=True)
        #    self.cache.set(key, costs, self.TTL)

        return prefix_costs
            
        

    
        