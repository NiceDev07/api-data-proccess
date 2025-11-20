from typing import Protocol, Dict, Any
from ..models.process_dto import BaseFileConfig

class IFileReader(Protocol):
    async def read(self, file: BaseFileConfig): ...
