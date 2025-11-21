from typing import Dict, Any
from modules.process.domain.interfaces.process import IDataProcessor
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.infrastructure.files.factory import ReaderFileFactory
from modules.process.domain.interfaces.level_validator import IUserLevelValidator  
from modules.data_processing.application.pipelines import (CleanData, ConcatPrefix)

class SmsProcessor(IDataProcessor):
    def __init__(self,
        cache,
        fileReaderfactory: ReaderFileFactory,
        levelValidator: IUserLevelValidator
    ):
        self.cache = cache
        self.fileReaderfactory = fileReaderfactory
        self.levelValidator = levelValidator
        self.steps = [
            CleanData(),
            ConcatPrefix(),
        ]

    async def process(self, payload: DataProcessingDTO) -> Dict[str, Any]:
        # configExclusion = getattr(payload.configListExclusion, "folder", None)
        file_result = await self.fileReaderfactory.create(payload.configFile.folder).read(payload.configFile)
        df = await self.levelValidator.validate(file_result, payload) # Devuelve df con los 10 primeros validados, o el df completo si es nivel > 1


        return {
            "status": "SMS processed",
            "success": True,
        }