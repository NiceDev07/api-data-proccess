from abc import ABC, abstractmethod
from typing import List, Optional
import polars as pl

class IFileReader(ABC):
    @abstractmethod
    def read(self, filepath: str, usecols: Optional[List[str]] = None, nrows: Optional[int] = None) ->  pl.DataFrame:
        pass

class ObjectReaderInterface(ABC):
    @abstractmethod
    def read(self, data: object) -> pl.DataFrame:
        pass