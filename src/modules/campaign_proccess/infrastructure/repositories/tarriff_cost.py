from logging import Logger, getLogger
from typing import Optional
from modules.campaign_proccess.domain.interfaces.tariff_repository import TariffRepositoryInterface
from core.cache.interfaces.cache_interface import CacheInterface
from core.db.interfaces.database import DatabaseSessionInterface
from modules.campaign_proccess.domain.entities.tariff import Tariff

class TariffRepository(TariffRepositoryInterface):
    def __init__(self, db: DatabaseSessionInterface, cache: CacheInterface, logger: Logger = None):
        self.db = db
        self.cache = cache
        self.logger = logger or getLogger(__name__)

    def get_tariff(self, country_id: int, tariff_id: int, service: str) -> Optional[Tariff]:
        key = f"tariff:{tariff_id}:{country_id}:{service}"
        updated_at = self.get_updated_at(tariff_id)
        cache_data = self.get_cache(key, update_at=updated_at)
        if cache_data:
            return cache_data
        
        return None
        
    def get_updated_at(self, tariff_id: int):
        query = "SELECT updated_at FROM saem3.w48fa_tariffs WHERE id = :tariff_id LIMIT 1"
        try:            
            self.logger.info(f"Getting updated_at for tariff_id: {tariff_id} table w48fa_tariffs")
            result = self.db.fetch_one(query, {"tariff_id": tariff_id})
            return result[0] if result else None
        except Exception as e:
            self.logger.error(f"Error fetching updated_at for tariff_id {tariff_id} Table w48fa_tariffs: {e}")
            return None

    def get_cache(self, key:str, update_at: str = None):
        cached_data = self.cache.get(key)
        if not cached_data:
            return None
        
        update_at_cache = cached_data.get("date_cache")

        if update_at == update_at_cache:
            return cached_data

        return None


