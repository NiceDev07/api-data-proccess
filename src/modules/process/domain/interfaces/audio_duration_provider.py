from typing import Protocol


class IAudioDurationProvider(Protocol):
    async def get_duration(self, path: str) -> float:
        """Retorna la duración del audio en segundos (con padding aplicado)."""
        ...
