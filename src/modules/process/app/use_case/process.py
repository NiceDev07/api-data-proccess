from modules.process.domain.interfaces.process import IDataProcessor
from modules.data_processing.application.schemas.preload_camp_schema import DataProcessingDTO

class ProcessDataUseCase:
    def __init__(self, processor: IDataProcessor):
        self.processor = processor

    async def __call__(self, payload: DataProcessingDTO):
        return await self.processor.process(payload)