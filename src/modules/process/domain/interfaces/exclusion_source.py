from typing import Protocol, List

class IExclusionSource(Protocol):
    async def get_numbers(self) -> list[str]:
        ...