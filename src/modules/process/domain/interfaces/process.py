from typing import Protocol, Dict, Any
from ..models.process_dto import DataProcessingDTO

class IDataProcessor(Protocol):
    async def process(self, payload: DataProcessingDTO) -> Dict[str, Any]: ...
