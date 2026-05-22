"""
Tests unitarios para CallBlastingConfirmRepository (AsyncEngine PostgreSQL).

Validan que:
  1. create_campaign_tables llama a la función details.create_campaign_table con el campaign_id correcto.
  2. bulk_insert apunta a details."campaign_{campaign_id}" con INSERT ... ON CONFLICT DO NOTHING.
  3. DataFrame vacío retorna 0 sin tocar la BD.
  4. El número de filas insertadas coincide con el tamaño del DataFrame.
"""
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest

from modules.process.infrastructure.repositories.callblasting_confirm import CallBlastingConfirmRepository

pytestmark = pytest.mark.anyio

CAMPAIGN_ID = 9_999_299


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_async_engine_mock():
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__  = AsyncMock(return_value=False)

    engine = MagicMock()
    engine.connect.return_value = conn
    engine.begin.return_value   = conn
    return engine, conn


def _sample_df(n: int = 2) -> pl.DataFrame:
    return pl.DataFrame({
        "celular":   [f"300000000{i}" for i in range(n)],
        "estado":    ["P"] * n,
        "operador":  ["CLARO"] * n,
        "segundos":  [30] * n,
        "credit":    [0.5] * n,
    })


def _sqls(conn) -> list[str]:
    return [str(c.args[0]) for c in conn.execute.call_args_list]


# ── tests ─────────────────────────────────────────────────────────────────────

async def test_create_tables_calls_function_with_correct_id():
    """create_campaign_tables llama a details.create_campaign_table con el campaign_id correcto."""
    engine, conn = _make_async_engine_mock()
    repo = CallBlastingConfirmRepository(engine=engine)

    await repo.create_campaign_tables([CAMPAIGN_ID])

    sp_calls = [c for c in conn.execute.call_args_list
                if "create_campaign_table" in str(c.args[0])]
    assert sp_calls, "details.create_campaign_table nunca fue llamado"
    assert sp_calls[0].args[1] == {"cid": CAMPAIGN_ID}


async def test_empty_df_returns_zero_without_db_call():
    """DataFrame vacío hace cortocircuito antes de cualquier llamada a BD."""
    engine, conn = _make_async_engine_mock()
    repo = CallBlastingConfirmRepository(engine=engine)

    result = await repo.bulk_insert(CAMPAIGN_ID, pl.DataFrame())

    assert result == 0
    conn.execute.assert_not_called()


async def test_bulk_insert_targets_correct_table():
    """INSERT apunta a details.\"campaign_{campaign_id}\"."""
    engine, conn = _make_async_engine_mock()
    repo = CallBlastingConfirmRepository(engine=engine)

    inserted = await repo.bulk_insert(CAMPAIGN_ID, _sample_df(2))

    assert inserted == 2
    sqls = _sqls(conn)
    inserts = [s for s in sqls if "INSERT" in s.upper()]
    assert inserts, "Ningún INSERT fue ejecutado"
    assert all(f"campaign_{CAMPAIGN_ID}" in s for s in inserts)


async def test_bulk_insert_uses_on_conflict_do_nothing():
    """ON CONFLICT DO NOTHING evita duplicados (sintaxis PostgreSQL)."""
    engine, conn = _make_async_engine_mock()
    repo = CallBlastingConfirmRepository(engine=engine)

    await repo.bulk_insert(CAMPAIGN_ID, _sample_df(2))

    sqls = _sqls(conn)
    inserts = [s for s in sqls if "INSERT" in s.upper()]
    assert inserts and all("ON CONFLICT DO NOTHING" in s.upper() for s in inserts)


async def test_bulk_insert_returns_row_count():
    """bulk_insert retorna el número de filas del DataFrame."""
    engine, conn = _make_async_engine_mock()
    repo = CallBlastingConfirmRepository(engine=engine)

    result = await repo.bulk_insert(CAMPAIGN_ID, _sample_df(5))

    assert result == 5
