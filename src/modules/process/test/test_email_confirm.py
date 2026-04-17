"""
Tests unitarios para EmailConfirmRepository.

Validan que:
  1. El SP create_mail_table se llama antes de insertar.
  2. Tanto batch INSERT como LOAD DATA apuntan a mail_{campaign_id}.
  3. El DataFrame vacío retorna 0 sin tocar la BD.

No se usa patch — el Engine se inyecta directamente en el constructor,
igual que lo haría FastAPI vía Depends(get_sync_engine_email).
Se usa campaign_id=9999991919 (número imposible en producción) para evitar colisiones.
"""
from unittest.mock import MagicMock

import polars as pl
import pytest

from modules.process.infrastructure.repositories.email_confirm import (
    EmailConfirmRepository,
    _LOAD_DATA_THRESHOLD,
)

pytestmark = pytest.mark.anyio

CAMPAIGN_ID = 1

# ── helpers ───────────────────────────────────────────────────────────────────

def _make_engine_mock():
    """Engine + connection con soporte de context manager."""
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine = MagicMock()
    engine.begin.return_value = ctx
    engine.connect.return_value = ctx
    return engine, conn


def _make_repo(engine=None) -> tuple[EmailConfirmRepository, MagicMock, MagicMock]:
    if engine is None:
        engine, conn = _make_engine_mock()
    else:
        _, conn = engine, MagicMock()
    eng, conn = _make_engine_mock()
    return EmailConfirmRepository(engine=eng), eng, conn


def _sample_df(n: int = 2) -> pl.DataFrame:
    """DataFrame mínimo con las columnas que produce EmailConfirmStrategy."""
    return pl.DataFrame({
        "mail":        [f"user{i}@test.com" for i in range(n)],
        "body":        ["Hello"] * n,
        "subject":     ["Subject"] * n,
        "id_client":   [""] * n,
        "name_client": [""] * n,
        "status":      ["P"] * n,
        "services":    [str(i % 100 + 1) for i in range(n)],
    })


def _sqls(conn_mock) -> list[str]:
    """Extrae los SQL (como string) de todas las llamadas a conn.execute."""
    return [str(c.args[0]) for c in conn_mock.execute.call_args_list]

# ── tests ─────────────────────────────────────────────────────────────────────

async def test_empty_df_returns_zero_without_db_call():
    """DataFrame vacío hace cortocircuito antes de cualquier llamada a BD."""
    engine, conn = _make_engine_mock()
    repo = EmailConfirmRepository(engine=engine)
    result = await repo.bulk_insert(CAMPAIGN_ID, pl.DataFrame())
    assert result == 0
    conn.execute.assert_not_called()


async def test_create_table_sp_called_with_correct_id():
    """SP create_mail_table se llama con el campaign_id exacto."""
    engine, conn = _make_engine_mock()
    await EmailConfirmRepository(engine=engine).bulk_insert(CAMPAIGN_ID, _sample_df())

    sp_calls = [c for c in conn.execute.call_args_list if "create_mail_table" in str(c.args[0])]
    assert sp_calls, "SP create_mail_table nunca fue llamado"
    assert sp_calls[0].args[1] == {"cid": CAMPAIGN_ID}


async def test_batch_insert_targets_dynamic_table():
    """INSERT IGNORE apunta a mail_{campaign_id}, no a ninguna tabla estática."""
    engine, conn = _make_engine_mock()
    inserted = await EmailConfirmRepository(engine=engine).bulk_insert(CAMPAIGN_ID, _sample_df(2))

    assert inserted == 2
    sqls = _sqls(conn)
    assert any(f"mail_{CAMPAIGN_ID}" in s for s in sqls), "Tabla dinámica no encontrada en SQLs"
    inserts = [s for s in sqls if s.strip().upper().startswith("INSERT")]
    assert all(f"mail_{CAMPAIGN_ID}" in s for s in inserts), "INSERT apuntó a tabla incorrecta"


async def test_load_data_targets_dynamic_table():
    """LOAD DATA LOCAL INFILE apunta a mail_{campaign_id} para >= threshold filas."""
    engine, conn = _make_engine_mock()
    n = _LOAD_DATA_THRESHOLD  # exactamente en el umbral → usa LOAD DATA

    inserted = await EmailConfirmRepository(engine=engine).bulk_insert(CAMPAIGN_ID, _sample_df(n))

    assert inserted == n
    sqls = _sqls(conn)
    load_calls = [s for s in sqls if "LOAD DATA" in s]
    assert load_calls, "LOAD DATA nunca fue llamado para el dataset grande"
    assert any(f"mail_{CAMPAIGN_ID}" in s for s in load_calls)
