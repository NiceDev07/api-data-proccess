from ..schemas.preload_camp_schema import PreloadCampDTO
from ..use_cases.sms_proccess import SMSUseCase
from core.file.file_reader import FileReader
from ..factories.rules_country import RulesCountryFactory
from modules.campaign_proccess.infrastructure.repositories.sql_forbidden_works import ForbiddenWordsRepository
from modules.campaign_proccess.domain.services.forbbiden_works import ForbiddenWordsService
from modules.campaign_proccess.infrastructure.cache.redis_cache import RedisCache

class SMSUseCaseBuilder:
    def __init__(
        self,
        payload: PreloadCampDTO,
        dbs: dict[str, any],  # type: ignore
        cache
    ):
        self.payload = payload
        self.dbs = dbs
        self.cache = cache

    def build(self) -> SMSUseCase:
        file_reader = FileReader().load(
            path=self.payload.configFile.folder,
            context={
                "sep": self.payload.configFile.delimiter,
                "header": self.payload.configFile.useHeaders
            }
        )
        db_forbidden = self.dbs.get("forbidden_words")
        forbidden_words_repo = ForbiddenWordsRepository(db_forbidden)
        rules_country = RulesCountryFactory.from_dto(self.payload.rulesCountry)
        redis_cache = RedisCache(self.cache)  # Assuming RedisCache is the cache implementation
        forbidden_words_service = ForbiddenWordsService(
            repo=forbidden_words_repo,
            cache=redis_cache
        )
        return SMSUseCase(
            file_reader=file_reader,
            rules_country=rules_country,
            forbidden_service=forbidden_words_service
        )
