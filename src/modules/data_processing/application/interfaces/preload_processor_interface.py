from abc import ABC, abstractmethod
from ..schemas.preload_camp_schema import DataProcessingDTO

class PreloadCampaignUseCaseInterface(ABC):
    @abstractmethod
    def execute(self, payload: DataProcessingDTO) -> dict:
        pass
