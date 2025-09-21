from modules.data_processing.domain.interfaces.cache_interface import ICache
from modules.data_processing.domain.interfaces.black_list_crc_repository import IBlackListCRCRepository

class RneService:
    KEY_CACHE = "list_rne:v1"
    TTL_SECONDS = 60*60

    def __init__(
        self,
        rne_repository: IBlackListCRCRepository,
        cache: ICache
    ):
        self.rne_repository = rne_repository
        self.cache = cache

    def __get_cache(self):
        list_cache = self.cache.get(self.KEY_CACHE)
        if list_cache is None:
            list_cache = set(self.rne_repository.get_black_list_crc())
            # self.cache.set(self.KEY_CACHE, list_cache, ttl=self.TTL_SECONDS) #!CORREGIR SERIALIZACION
        
        return list_cache
    
    def get_list_rne(self):
        return self.__get_cache()
        