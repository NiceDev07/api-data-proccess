from core.file.validators.local_file_validator import LocalFileValidator
from core.file.interfaces.file_validator import FileValidatorInterface


class SMSUseCase:
    def __init__(
        self,
        file_validator: FileValidatorInterface
    ):
        pass

    def excute(self, payload):
        return {"status": "SMS campaign processed successfully", "campaign_id": payload.get("campaign_id")}