from typing import Protocol
from modules.process.domain.interfaces.process import IDataProcessor
from modules.process.domain.enums.services import ServiceType

class IProcessorFactory(Protocol):
    def create(self, service: ServiceType) -> IDataProcessor:
        ...
