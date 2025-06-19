from modules._common.infrastructure.files.file_reader_factory import FileReaderFactory
from modules.campaign_proccess.infrastructure.repositories.sql_forbidden_works import ForbiddenWordsRepository
from modules.campaign_proccess.infrastructure.cache.redis_cache import RedisCache
from modules.campaign_proccess.domain.services.forbbiden_works import ForbiddenWordsService
from modules.campaign_proccess.application.services.dataframe_preprocessor import DataFramePreprocessor
from modules.campaign_proccess.application.use_cases.sms_proccess import SMSUseCase
from modules.campaign_proccess.application.schemas.preload_camp_schema import PreloadCampDTO
from modules.campaign_proccess.infrastructure.repositories.black_list_crc_repository import BlackListCRCRepository

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
        file_reader = FileReaderFactory.get_reader(
            self.payload.configFile.folder,
            context={
                "sep": self.payload.configFile.delimiter,
                "header": self.payload.configFile.useHeaders
            }
        )
        db_filters = self.dbs.get("filter_db")
        forbidden_words_repo = ForbiddenWordsRepository(db_filters)
        blacklist_crc_repo = BlackListCRCRepository(db_filters)

        redis_cache = RedisCache(self.cache)  # Assuming RedisCache is the cache implementation
        forbidden_words_service = ForbiddenWordsService(
            repo=forbidden_words_repo,
            cache=redis_cache
        )
        df_processor = DataFramePreprocessor(file_reader)
        return SMSUseCase(
            df_processor=df_processor,
            forbidden_service=forbidden_words_service,
            blacklist_crc_repo=blacklist_crc_repo
        )
