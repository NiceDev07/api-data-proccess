from modules.process.domain.enums.services import ServiceType
from modules.process.infrastructure.process.sms import SmsProcessor
from modules.process.infrastructure.process.callblasting import CallBlastingProcess
from modules.process.infrastructure.files.factory import ReaderFileFactory

class ProcessorFactory:
    def __init__(self, cache):
        self.reader_factory = ReaderFileFactory()
        self.cache = cache
        self._map = {
            ServiceType.sms: SmsProcessor,
            ServiceType.call_blasting: CallBlastingProcess,
        }

    def create(self, service: ServiceType):
        cls = self._map.get(service)
        if not cls:
            raise ValueError(f"Servicio no soportado: {service}")
        return cls(self.cache, self.reader_factory)