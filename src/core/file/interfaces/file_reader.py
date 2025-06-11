from abc import ABC, abstractmethod
from typing import List, Optional
from dask import dataframe as dd

class FileReaderInterface(ABC):
    @abstractmethod
    def read(self, filepath: str, usecols: Optional[List[str]] = None, nrows: Optional[int] = None) ->  dd.DataFrame:
        pass

class ObjectReaderInterface(ABC):
    @abstractmethod
    def read(self, data: object) -> dd.DataFrame:
        pass