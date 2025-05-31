from abc import ABC, abstractmethod
import dask.dataframe as dd

class ProcessorInterface(ABC):
    @abstractmethod
    def process(self, df: dd.DataFrame) -> dd.DataFrame:
        """
        Aplica el procesamiento específico del servicio.
        """
        pass