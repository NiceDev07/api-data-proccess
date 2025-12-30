from modules.process.domain.enums.services import ServiceType
from modules.process.app.process.sms import SmsProcessor
from modules.process.app.process.callblasting import CallBlastingProcess


class ProcessorFactory:
    def __init__(self, processors: dict):
        self._map = processors

    def create(self, service: ServiceType):
        cls = self._map.get(service)
        if not cls:
            raise ValueError(f"Servicio no soportado: {service}")
        return cls