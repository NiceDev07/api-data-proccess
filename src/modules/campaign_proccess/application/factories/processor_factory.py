# application/factory/processor_factory.py

from ..use_cases.sms_proccess import SMSUseCase

from typing import Callable

from core.file.validators.local_file_validator import LocalFileValidator
from core.file.readers.csv_reader import CSVReader, JSONReader
from core.cache.redis_adapter import RedisCache
from ..interfaces.preload_processor_interface import PreloadCampaignUseCaseInterface
# application/factory/processor_factory.py

class CampaignProcessorFactory:
    def __init__(self, db, cache, sms_db=None):
        self.db = db
        self.cache = cache
        self.sms_db = sms_db or db

        self._register: dict[str, Callable[[], PreloadCampaignUseCaseInterface]] = {
            "sms": self._build_sms_processor,
            # "email": self._build_email_processor,
            # "call_blasting": self._build_call_blasting_processor,
            # "api_call_blasting": self._build_api_call_blasting_processor
        }

    def _build_sms_processor(self):
        return SMSUseCase(
            file_validator=LocalFileValidator(),
            file_reader=CSVReader(),
            blacklist_cache=RedisCache(self.cache),
            db=self.sms_db
        )

    # def _build_email_processor(self):
    #     return EmailCampaignProcessorUseCase(
    #         file_validator=LocalFileValidator(),
    #         file_reader=CSVReader(),
    #         blacklist_cache=RedisCache(self.cache),
    #         db=self.db
    #     )

    # def _build_call_blasting_processor(self):
    #     return CallBlastingCampaignProcessorUseCase(
    #         file_validator=LocalFileValidator(),
    #         file_reader=CSVReader(),
    #         blacklist_cache=RedisCache(self.cache),
    #         db=self.db
    #     )

    # def _build_api_call_blasting_processor(self):
    #     return CallBlastingCampaignProcessorUseCase(
    #         file_reader=JSONReader(),
    #         blacklist_cache=RedisCache(self.cache),
    #         db=self.db
    #     )

    def create(self, service: str) -> PreloadCampaignUseCaseInterface:
        builder = self._register.get(service)
        if builder is None:
            raise ValueError(f"Servicio '{service}' no soportado")
        return builder()
