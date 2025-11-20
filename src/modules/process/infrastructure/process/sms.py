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
        file_reader = self.fileReaderfactory.create(payload.configFile)
        # exclution_reader = self.fileReaderfactory.create(payload.configListExclusion)
        file_result = await file_reader.read(payload.configFile)
        # exclution_result = await exclution_reader.read(payload.configListExclusion)

        return {
            "status": "SMS processed",
            "success": True,
        }