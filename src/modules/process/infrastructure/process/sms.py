from typing import Dict, Any
from modules.process.domain.interfaces.process import IDataProcessor
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.infrastructure.files.factory import ReaderFileFactory

class SmsProcessor(IDataProcessor):
    def __init__(self,
        cache,
        fileReaderfactory: ReaderFileFactory
    ):
        self.cache = cache
        self.fileReaderfactory = fileReaderfactory

    async def process(self, payload: DataProcessingDTO) -> Dict[str, Any]:
        file_result = await self.fileReaderfactory.create(payload.configFile.folder).read(payload.configFile)

        return {
            "status": "SMS processed",
            "success": True,
        }