from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.interfaces.level_validator import IUserLevelValidator
from modules.process.app.interfaces import (IFileReaderFactory, IProcessorFactory)
from modules.process.domain.enums.services import ServiceType

class ProcessDataUseCase:
    def __init__(
        self,
        processor_factory: IProcessorFactory,
        file_reader_factory: IFileReaderFactory,
        level_validator: IUserLevelValidator
    ):
        self.processor_factory = processor_factory
        self.reader_factory = file_reader_factory
        self.level_validator = level_validator

    async def __call__(
        self,
        service: ServiceType, 
        payload: DataProcessingDTO
    ):
        processor = self.processor_factory.create(service)
        reader = self.reader_factory.create(payload.configFile.folder)
        lf = await reader.read(payload.configFile)
        lf = await self.level_validator.validate(lf, payload)
        return await processor.process(lf, payload)