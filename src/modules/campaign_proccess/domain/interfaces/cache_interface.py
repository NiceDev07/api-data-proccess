from abc import ABC, abstractmethod
from typing import Optional, Any

class CacheInterface(ABC):
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    def set(self, key: str, value: Any, ttl: int = 3600) -> None:
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        pass
