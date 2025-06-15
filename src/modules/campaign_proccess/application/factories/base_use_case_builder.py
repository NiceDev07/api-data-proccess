from abc import ABC, abstractmethod

class IUseCase(ABC):
    @abstractmethod
    def execute(self, *args, **kwargs):
        """
        Método que ejecuta la lógica del caso de uso.
        Debe ser implementado por las subclases.
        """
        pass


class IUseCaseBuilder(ABC):
    @abstractmethod
    def build(self) -> IUseCase:
        """
        Construye y devuelve una instancia completa del caso de uso.
        Debe resolver todas las dependencias requeridas.
        """
        pass
