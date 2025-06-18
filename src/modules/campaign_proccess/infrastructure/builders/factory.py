from ..builders.base_use_case_builder import IUseCaseBuilder
from ..builders.sms_use_case_builder import SMSUseCaseBuilder

class UseCaseFactory:
    _builders: dict[str, type[IUseCaseBuilder]] = {}

    @classmethod
    def register(cls, service: str, builder_cls: type[IUseCaseBuilder]):
        cls._builders[service] = builder_cls

    @classmethod
    def create(cls, service: str, payload, dbs, cache):
        builder_cls = cls._builders.get(service)
        if not builder_cls:
            raise ValueError(f"Servicio no soportado: {service}")
        return builder_cls(payload, dbs, cache).build()



UseCaseFactory.register('sms', SMSUseCaseBuilder)
