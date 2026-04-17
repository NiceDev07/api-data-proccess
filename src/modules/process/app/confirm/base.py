from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from modules.process.infrastructure.storage.local import LocalStorage


class BaseConfirmStrategy(ABC):
    @property
    @abstractmethod
    def _service_name(self) -> str: ...

    @abstractmethod
    async def _do_confirm(self, path: Path, campaign_ids: list[int]) -> dict[str, Any]: ...

    def __init__(self, storage: LocalStorage):
        self._storage = storage

    def _build_path(self, key: str) -> Path:
        return Path(self._storage.base_dir) / f"Campaign/{self._service_name}/campaign_{key}.parquet"

    async def confirm(self, campaign_ids: list[int], code_group: str | None = None) -> dict[str, Any]:
        if code_group:
            path = self._build_path(code_group)
            if path.exists():
                return await self._do_confirm(path, campaign_ids)

        fallback_key = "-".join(str(c) for c in campaign_ids)
        path = self._build_path(fallback_key)
        if not path.exists():
            raise FileNotFoundError(
                f"Archivo de campaña no encontrado: {path.name}"
            )
        return await self._do_confirm(path, campaign_ids)
