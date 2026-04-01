from enum import Enum
from typing import Union
from modules.process.domain.enums.services import ServiceType


class SmsSubService(str, Enum):
    standard = "informative"
    landing  = "landing"


class CallBlastingSubService(str, Enum):
    standard = "standard"
    custom   = "custom"


class EmailSubService(str, Enum):
    standard = "standard"


class ApiCallSubService(str, Enum):
    standard = "standard"


# Alias de documentación — NO usar como tipo de campo Pydantic ya que los valores
# se solapan entre servicios ("standard" existe en varios enums).
# Cada procesador valida subService contra su propio enum.
SubService = Union[SmsSubService, CallBlastingSubService, EmailSubService, ApiCallSubService]


SUB_SERVICES: dict[ServiceType, type[Enum]] = {
    ServiceType.sms:           SmsSubService,
    ServiceType.call_blasting: CallBlastingSubService,
    ServiceType.email:         EmailSubService,
    ServiceType.api_call:      ApiCallSubService,
}
