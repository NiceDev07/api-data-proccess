from modules.process.domain.enums.services import ServiceType
from modules.process.domain.interfaces.confirm import IConfirmStrategy


class ConfirmFactory:
    def __init__(self, strategies: dict[ServiceType, IConfirmStrategy]):
        self._strategies = strategies

    def get(self, service: ServiceType) -> IConfirmStrategy:
        strategy = self._strategies.get(service)
        if strategy is None:
            raise ValueError(f"Servicio no soportado para confirmación: {service}")
        return strategy
