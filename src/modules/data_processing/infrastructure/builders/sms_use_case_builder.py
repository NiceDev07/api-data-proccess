from redis import Redis
from modules._common.infrastructure.files.file_reader_factory import FileReaderFactory
from modules.data_processing.infrastructure.repositories.sql_forbidden_works import ForbiddenWordsRepository
from modules.data_processing.infrastructure.cache.redis_cache import RedisCache
from modules.data_processing.application.services.forbbiden_works import ForbiddenWordsService
from modules.data_processing.application.services.dataframe_preprocessor import DataFramePreprocessor
from modules.data_processing.application.use_cases.sms_proccess import SMSUseCase
from modules.data_processing.infrastructure.repositories.black_list_crc_repository import BlackListCRCRepository
from modules.data_processing.application.schemas.preload_camp_schema import DataProcessingDTO
from modules.data_processing.infrastructure.db.db_context import DbContext
from modules.data_processing.infrastructure.repositories.sql_numeracion import NumeracionRepository
from modules.data_processing.infrastructure.repositories.sql_tariff_cost import CostRepository

class SMSUseCaseBuilder:
    def __init__(
        self,
        payload: DataProcessingDTO,
        dbs: DbContext,  # type: ignore
        cache: Redis
    ):
        self.payload = payload
        self.dbs = dbs
        self.cache = cache

    def build(self):
        db_filters = self.dbs.masivos_sms
        forbidden_words_repo = ForbiddenWordsRepository(db_filters)
        blacklist_crc_repo = BlackListCRCRepository(db_filters)
        redis_cache = RedisCache(self.cache)  # Assuming RedisCache is the cache implementation
        forbidden_words_service = ForbiddenWordsService(
            repo=forbidden_words_repo,
            cache=redis_cache
        )
        
        file_reader = FileReaderFactory.get_reader(configFile=self.payload.configFile)
        if self.payload.configListExclusion is not None:
            exclusion_reader = FileReaderFactory.get_reader(configFile=self.payload.configListExclusion)

        df_processor = DataFramePreprocessor(file_reader)
        numeracion_repo = NumeracionRepository(self.dbs.portabilidad_db)
        tariff_repo = CostRepository(self.dbs.saem3)

        return SMSUseCase(
            df_processor=df_processor,
            forbidden_service=forbidden_words_service,
            blacklist_crc_repo=blacklist_crc_repo,
            exclusion_reader=exclusion_reader if self.payload.configListExclusion else None,
            numeracion_repo=numeracion_repo,
            tariff_repo=tariff_repo
        )
