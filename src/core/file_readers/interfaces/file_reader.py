from abc import ABC, abstractmethod
from typing import List, Optional

class FileReaderInterface(ABC):
    @abstractmethod
    def read(self, filepath: str, usecols: Optional[List[str]] = None, nrows: Optional[int] = None) -> List:
        pass

class ObjectReaderInterface(ABC):
    @abstractmethod
    def read(self, data: object) -> List:
        pass