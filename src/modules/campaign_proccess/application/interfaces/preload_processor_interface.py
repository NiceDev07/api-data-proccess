from abc import ABC, abstractmethod
from ..schemas.preload_camp_schema import PreloadCampDTO

class PreloadCampaignUseCaseInterface(ABC):
    @abstractmethod
    def execute(self, payload: PreloadCampDTO) -> dict:
        pass
