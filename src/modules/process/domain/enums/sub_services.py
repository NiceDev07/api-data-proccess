from enum import Enum
from modules.process.domain.enums.services import ServiceType


class SmsSubService(str, Enum):
    standard = "sms_standard"
    landing = "sms_landing"


class EmailSubService(str, Enum):
    standard = "email_standard"


class CallBlastingSubService(str, Enum):
    standard = "call_blasting_standard"
    custom = "call_blasting_custom"


class ApiCallSubService(str, Enum):
    standard = "api_call_standard"


SUB_SERVICES: dict[ServiceType, type[Enum]] = {
    ServiceType.sms: SmsSubService,
    ServiceType.email: EmailSubService,
    ServiceType.call_blasting: CallBlastingSubService,
    ServiceType.api_call: ApiCallSubService,
}
