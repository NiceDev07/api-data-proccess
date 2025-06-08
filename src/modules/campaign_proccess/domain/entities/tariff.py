from typing import Optional

class Tariff:
    def __init__(
        self,
        type: str,
        prefix: str,
        operator: str,
        sms: float,
        cb_standard: float,
        cb_custom: float,
        email: float,
        initial: float,
        incremental: float,
        tariff_id: int,
        country_id: int,
    ):
        self.type = type
        self.prefix = prefix
        self.operator = operator
        self.sms = sms
        self.cb_standard = cb_standard
        self.cb_custom = cb_custom
        self.email = email
        self.initial = initial
        self.incremental = incremental
        self.tariff_id = tariff_id
        self.country_id = country_id

    def is_valid_tariff(self)-> bool:
        return self.tariff_id is not None
