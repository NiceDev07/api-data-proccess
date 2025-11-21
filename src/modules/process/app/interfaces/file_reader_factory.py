from typing import Protocol
from modules.process.domain.interfaces.file_reader import IFileReader

class IFileReaderFactory(Protocol):
    def create(self, file: str) -> IFileReader:
        ...
