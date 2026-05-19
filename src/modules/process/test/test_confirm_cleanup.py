"""
Tests unitarios para la lógica de limpieza de parquet en BaseConfirmStrategy.

Validan que:
  1. El parquet se elimina tras una inserción exitosa.
  2. El parquet NO se elimina si _do_confirm lanza excepción.
  3. Si path.unlink() falla, se loguea warning y se retorna el resultado igualmente.
  4. Si el parquet no existe, se lanza FileNotFoundError sin intentar eliminar.
  5. codeGroup tiene prioridad sobre campaignId al buscar el parquet.
"""
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from modules.process.app.confirm.base import BaseConfirmStrategy
from modules.process.infrastructure.storage.local import LocalStorage

pytestmark = pytest.mark.anyio


# ── implementación mínima de BaseConfirmStrategy para tests ───────────────────

class _FakeStrategy(BaseConfirmStrategy):
    _service_name = "test_svc"

    def __init__(self, storage: LocalStorage, do_confirm_result=None, do_confirm_raises=None):
        super().__init__(storage)
        self._result = do_confirm_result or {"inserted": 5}
        self._raises = do_confirm_raises

    async def _do_confirm(self, path: Path, campaign_ids: list[int]) -> dict[str, Any]:
        if self._raises:
            raise self._raises
        return self._result


# ── fixture: storage apuntando a un directorio temporal ──────────────────────

@pytest.fixture
def tmp_storage(tmp_path):
    storage = MagicMock(spec=LocalStorage)
    storage.base_dir = str(tmp_path)
    return storage, tmp_path


def _write_parquet(base: Path, service: str, key: str) -> Path:
    """Crea un parquet mínimo válido en la ruta esperada por _build_path."""
    path = base / f"Campaign/{service}/campaign_{key}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"x": [1, 2, 3]}).write_parquet(path)
    return path


# ── tests ─────────────────────────────────────────────────────────────────────

async def test_parquet_deleted_after_successful_confirm(tmp_storage):
    """El parquet se elimina cuando _do_confirm termina sin error."""
    storage, base = tmp_storage
    key = "999-1000"
    path = _write_parquet(base, "test_svc", key)
    assert path.exists()

    strategy = _FakeStrategy(storage, do_confirm_result={"inserted": 5})
    result = await strategy.confirm([999, 1000])

    assert result == {"inserted": 5}
    assert not path.exists(), "El parquet debería haberse eliminado tras el confirm exitoso"


async def test_parquet_not_deleted_on_confirm_error(tmp_storage):
    """El parquet se conserva si _do_confirm lanza excepción."""
    storage, base = tmp_storage
    key = "999"
    path = _write_parquet(base, "test_svc", key)
    assert path.exists()

    strategy = _FakeStrategy(storage, do_confirm_raises=RuntimeError("DB error"))
    with pytest.raises(RuntimeError, match="DB error"):
        await strategy.confirm([999])

    assert path.exists(), "El parquet NO debe eliminarse si hubo error en la inserción"


async def test_unlink_failure_logs_warning_and_returns_result(tmp_storage):
    """Si path.unlink() falla, se loguea warning pero se retorna el resultado."""
    storage, base = tmp_storage
    key = "999"
    path = _write_parquet(base, "test_svc", key)

    strategy = _FakeStrategy(storage, do_confirm_result={"inserted": 3})

    with patch.object(Path, "unlink", side_effect=OSError("permission denied")):
        with patch("modules.process.app.confirm.base.logger") as mock_logger:
            result = await strategy.confirm([999])

    assert result == {"inserted": 3}
    mock_logger.warning.assert_called_once()
    warning_msg = mock_logger.warning.call_args[0][0]
    assert "no se pudo eliminar" in warning_msg


async def test_file_not_found_raises_without_unlink(tmp_storage):
    """Si el parquet no existe, se lanza FileNotFoundError sin intentar eliminar."""
    storage, base = tmp_storage

    strategy = _FakeStrategy(storage)
    with pytest.raises(FileNotFoundError, match="FILE_NOT_FOUND"):
        await strategy.confirm([999])


async def test_code_group_parquet_used_when_exists(tmp_storage):
    """Si el parquet de codeGroup existe, se usa ese en lugar del de campaignId."""
    storage, base = tmp_storage
    path_group = _write_parquet(base, "test_svc", "MYGROUP01")

    strategy = _FakeStrategy(storage, do_confirm_result={"inserted": 7})
    result = await strategy.confirm([999], code_group="MYGROUP01")

    assert result == {"inserted": 7}
    assert not path_group.exists(), "El parquet de codeGroup debe eliminarse tras confirm exitoso"


async def test_fallback_to_campaign_id_when_code_group_not_found(tmp_storage):
    """Si el parquet de codeGroup no existe, cae al de campaignId."""
    storage, base = tmp_storage
    path_campaign = _write_parquet(base, "test_svc", "999")

    strategy = _FakeStrategy(storage, do_confirm_result={"inserted": 4})
    result = await strategy.confirm([999], code_group="INEXISTENTE")

    assert result == {"inserted": 4}
    assert not path_campaign.exists(), "El parquet de campaignId debe eliminarse en el fallback"
