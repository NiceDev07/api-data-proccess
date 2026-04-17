from typing import Protocol, Any


class IConfirmStrategy(Protocol):
    async def confirm(self, campaign_ids: list[int], code_group: str | None = None) -> dict[str, Any]:
        """Valida el archivo de campaña y ejecuta la confirmación de envío."""
        ...
