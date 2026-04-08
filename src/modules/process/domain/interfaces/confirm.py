from typing import Protocol, Any


class IConfirmStrategy(Protocol):
    async def confirm(self, campaign_ids: list[int]) -> dict[str, Any]:
        """Valida el archivo de campaña y ejecuta la confirmación de envío."""
        ...
