"""
Tests unitarios para EmailConfirmRepository (AsyncEngine).

Validan que:
  1. El SP create_mail_table se llama antes de insertar.
  2. El bulk insert apunta a mail_{campaign_id} con INSERT IGNORE.
  3. El DataFrame vacío retorna 0 sin tocar la BD.

El AsyncEngine se inyecta directamente (igual que FastAPI vía Depends).
Se usa CAMPAIGN_ID = 9_999_919 (número imposible en producción) para evitar colisiones.
"""
from unittest.mock import AsyncMock, MagicMock, call

import polars as pl
import pytest

from modules.process.infrastructure.repositories.email_confirm import EmailConfirmRepository

pytestmark = pytest.mark.anyio

CAMPAIGN_ID = 9_999_919


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_async_engine_mock():
    """AsyncEngine + connection async con soporte de context manager."""
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__  = AsyncMock(return_value=False)

    engine = MagicMock()
    engine.connect.return_value = conn
    engine.begin.return_value   = conn
    return engine, conn


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
        "ip_client":   [""] * n,
        "country":     [""] * n,
        "city":        [""] * n,
    })


def _sqls(conn_mock) -> list[str]:
    """Extrae los SQL (como string) de todas las llamadas a conn.execute."""
    return [str(c.args[0]) for c in conn_mock.execute.call_args_list]


# ── tests ─────────────────────────────────────────────────────────────────────

async def test_empty_df_returns_zero_without_db_call():
    """DataFrame vacío hace cortocircuito antes de cualquier llamada a BD."""
    engine, conn = _make_async_engine_mock()
    repo = EmailConfirmRepository(engine=engine)
    result = await repo.bulk_insert(CAMPAIGN_ID, pl.DataFrame())
    assert result == 0
    conn.execute.assert_not_called()


async def test_create_table_sp_called_with_correct_id():
    """SP create_mail_table se llama con el campaign_id exacto."""
    engine, conn = _make_async_engine_mock()
    await EmailConfirmRepository(engine=engine).create_campaign_tables([CAMPAIGN_ID])

    sp_calls = [c for c in conn.execute.call_args_list if "create_mail_table" in str(c.args[0])]
    assert sp_calls, "SP create_mail_table nunca fue llamado"
    assert sp_calls[0].args[1] == {"cid": CAMPAIGN_ID}


async def test_bulk_insert_targets_dynamic_table():
    """INSERT IGNORE apunta a mail_{campaign_id}, no a ninguna tabla estática."""
    engine, conn = _make_async_engine_mock()
    inserted = await EmailConfirmRepository(engine=engine).bulk_insert(CAMPAIGN_ID, _sample_df(2))

    assert inserted == 2
    sqls = _sqls(conn)
    inserts = [s for s in sqls if "INSERT" in s.upper()]
    assert inserts, "Ningún INSERT fue ejecutado"
    assert all(f"mail_{CAMPAIGN_ID}" in s for s in inserts), (
        f"INSERT no apuntó a mail_{CAMPAIGN_ID}"
    )


async def test_bulk_insert_adds_id_campaign_column():
    """El repositorio agrega id_campaign al DataFrame antes de insertar."""
    engine, conn = _make_async_engine_mock()
    df = _sample_df(2)
    assert "id_campaign" not in df.columns

    await EmailConfirmRepository(engine=engine).bulk_insert(CAMPAIGN_ID, df)

    insert_calls = [c for c in conn.execute.call_args_list if "INSERT" in str(c.args[0]).upper()]
    assert insert_calls, "No se encontraron llamadas INSERT"
    rows = insert_calls[0].args[1]
    assert isinstance(rows, list) and len(rows) > 0
    assert "id_campaign" in rows[0], "id_campaign no fue agregado al payload de inserción"
    assert rows[0]["id_campaign"] == CAMPAIGN_ID


async def test_bulk_insert_includes_location_fields():
    """ip_client, country y city se incluyen en el payload de inserción."""
    engine, conn = _make_async_engine_mock()
    await EmailConfirmRepository(engine=engine).bulk_insert(CAMPAIGN_ID, _sample_df(2))

    insert_calls = [c for c in conn.execute.call_args_list if "INSERT" in str(c.args[0]).upper()]
    assert insert_calls, "No se encontraron llamadas INSERT"
    rows = insert_calls[0].args[1]
    assert isinstance(rows, list) and len(rows) > 0
    for field in ("ip_client", "country", "city"):
        assert field in rows[0], f"'{field}' no fue incluido en el payload de inserción"
        assert rows[0][field] == "", f"'{field}' debería ser string vacío"


async def test_bulk_insert_applies_session_optimizations():
    """SET SESSION unique_checks=0 se ejecuta antes de insertar."""
    engine, conn = _make_async_engine_mock()
    await EmailConfirmRepository(engine=engine).bulk_insert(CAMPAIGN_ID, _sample_df(2))

    sqls = _sqls(conn)
    assert any("unique_checks=0" in s for s in sqls), (
        "SET SESSION unique_checks=0 no fue ejecutado"
    )
