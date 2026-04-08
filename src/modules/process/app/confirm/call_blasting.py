from pathlib import Path
from typing import Any

from modules.process.infrastructure.storage.local import LocalStorage
from modules.process.app.confirm.base import BaseConfirmStrategy


class CallBlastingConfirmStrategy(BaseConfirmStrategy):
    _service_name = "call_blasting"

    def __init__(self, storage: LocalStorage):
        super().__init__(storage)

    async def _do_confirm(self, path: Path, campaign_ids: list[int]) -> dict[str, Any]:
        # TODO: implementar lógica de confirmación de envío Call Blasting
        return {"confirmed": campaign_ids}
