from dataclasses import dataclass

@dataclass
class Tariff:
    type: str
    prefix: str
    operator: str
    sms: float
    cb_standard: float
    cb_custom: float
    email: float
    initial: float
    incremental: float
    tariff_id: int
    country_id: int

    def is_valid_tariff(self)-> bool:
        return self.tariff_id is not None
