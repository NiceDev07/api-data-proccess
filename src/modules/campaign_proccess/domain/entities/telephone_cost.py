from enum import Enum
from dataclasses import dataclass

class TypeEnum(Enum):
    MOBILE = "mobile"
    FIXED = "fixed"

@dataclass
class TelephoneCosts:
    type: TypeEnum
    prefix: str
    operator: str
    sms_cost: float
    call_standard_cost: float
    call_custom_cost: float
    email_cost: float
    initial: int
    increment: int
    tarriff_id: int
    country_id: int
