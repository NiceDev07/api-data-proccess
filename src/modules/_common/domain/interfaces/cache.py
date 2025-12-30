from abc import ABC, abstractmethod
from typing import Any, Optional


class ICache(ABC):
    """
    Port de cache.
    La implementación decide cómo serializar.
    """

    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """
        Retorna el valor cacheado o None si no existe.
        """
        ...

    @abstractmethod
    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        """
        Guarda un valor en cache con TTL (en segundos).
        """
        ...

    @abstractmethod
    async def delete(self, key: str) -> None:
        """
        Elimina una clave del cache.
        """
        ...

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """
        Indica si una clave existe en cache.
        """
        ...
