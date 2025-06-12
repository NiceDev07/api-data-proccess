from core.file.interfaces.file_validator import FileValidatorInterface
from ..schemas.preload_camp_schema import PreloadCampDTO
from ..interfaces.preload_processor_interface import PreloadCampaignUseCaseInterface
from typing import Literal

class PreloadCampaignsUseCase:
    _register: dict[Literal["sms", "api"], type[PreloadCampaignUseCaseInterface]] = {
        "sms": None,
        "api": None
    }

    def __init__(
        self,
        file_validator: FileValidatorInterface
    ):
        self.file_validator = file_validator

    def execute(self, payload: PreloadCampDTO):
        self.file_validator.validate(payload.configFile.folder)

        # Logic to preload campaigns goes here
        return {"campaign_id": 200}