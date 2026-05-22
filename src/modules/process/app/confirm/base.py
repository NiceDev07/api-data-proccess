from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from modules.process.domain.interfaces.storage import IStorage
from logging_config import get_logger

logger = get_logger(__name__)


class BaseConfirmStrategy(ABC):
    @property
    @abstractmethod
    def _service_name(self) -> str: ...

    @abstractmethod
    async def _do_confirm(self, path: Path, campaign_ids: list[int]) -> dict[str, Any]: ...

    def __init__(self, storage: IStorage):
        self._storage = storage

    def _build_path(self, key: str) -> Path:
        return Path(self._storage.base_dir) / f"Campaign/{self._service_name}/campaign_{key}.parquet"

    async def confirm(self, campaign_ids: list[int], code_group: str | None = None) -> dict[str, Any]:
        if code_group:
            path = self._build_path(code_group)
            if path.exists():
                return await self._confirm_and_cleanup(path, campaign_ids)

        fallback_key = "-".join(str(c) for c in campaign_ids)
        path = self._build_path(fallback_key)
        if not path.exists():
            logger.error("Confirm [%s] | archivo no encontrado | campaigns=%s | path=%s", self._service_name, campaign_ids, path)
            raise FileNotFoundError("FILE_NOT_FOUND: Campaign file not found.")
        return await self._confirm_and_cleanup(path, campaign_ids)

    async def _confirm_and_cleanup(self, path: Path, campaign_ids: list[int]) -> dict[str, Any]:
        result = await self._do_confirm(path, campaign_ids)
        try:
            path.unlink(missing_ok=True)
        except Exception:
            # El archivo ya fue procesado; si no se puede borrar no es crítico
            logger.warning("Confirm [%s] | no se pudo eliminar parquet | path=%s", self._service_name, path)
        return result
