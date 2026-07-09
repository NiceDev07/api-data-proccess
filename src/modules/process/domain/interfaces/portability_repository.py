from abc import ABC, abstractmethod
from typing import Optional


class IPortabilityRepository(ABC):
    @abstractmethod
    async def consulta_operador(
        self,
        celular: str,
        code_flash: str,
        gcode: int,
        national_len: int,
        id_pais: int,
    ) -> Optional[dict]:
        """Resuelve operador + routing + portabilidad de un número (flujo unitario)."""
        pass
