from enum import Enum

class ServiceType(str, Enum):
    sms = "sms"
    email = "email"
    call_blasting = "call_blasting"
    api_call = "api_call"