from ..schemas.preload_camp_schema import PreloadCampDTO
from ..use_cases.sms_proccess import SMSUseCase
from core.file.file_reader import FileReader

class SMSUseCaseBuilder:
    def __init__(
        self,
        payload: PreloadCampDTO,
        db,
        cache
    ):
        self.payload = payload
        self.db = db
        self.cache = cache

    def build(self) -> SMSUseCase:
        file_reader = FileReader().load(
            path=self.payload.configFile.folder,
            context={
                "sep": self.payload.configFile.delimiter,
                "header": self.payload.configFile.useHeaders
            }
        )
        return SMSUseCase(
            file_reader=file_reader,
        )
