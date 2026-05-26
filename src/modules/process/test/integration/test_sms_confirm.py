"""
Tests unitarios para SmsConfirmRepository (AsyncEngine).

Validan que:
  1. create_campaign_tables llama al SP create_campaign_table con el campaign_id correcto.
  2. bulk_insert apunta a campana_{campaign_id} con INSERT IGNORE.
  3. DataFrame vacío retorna 0 sin tocar la BD.
  4. SET SESSION unique_checks=0 se aplica antes de insertar.
"""
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest

from modules.process.infrastructure.repositories.confirm.sms import SmsConfirmRepository

pytestmark = pytest.mark.anyio

CAMPAIGN_ID = 9_999_191


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_session_mock():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit  = AsyncMock()
    return session


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
        "celular":        [f"300000000{i}" for i in range(n)],
        "id_campana":     [CAMPAIGN_ID] * n,
        "estado":         ["P"] * n,
        "texto":          ["Hola test"] * n,
        "identificacion": [""] * n,
        "servicio":       [str(i + 1) for i in range(n)],
        "operador":       ["CLARO"] * n,
        "pdu":            [1] * n,
        "credit":         [0.5] * n,
    })


def _sqls(conn) -> list[str]:
    return [str(c.args[0]) for c in conn.execute.call_args_list]


# ── tests ─────────────────────────────────────────────────────────────────────

async def test_create_tables_calls_sp_with_correct_id():
    """create_campaign_tables llama al SP con el campaign_id correcto."""
    session = _make_session_mock()
    engine, _ = _make_async_engine_mock()
    repo = SmsConfirmRepository(session=session, async_engine=engine)

    await repo.create_campaign_tables([CAMPAIGN_ID])

    sp_calls = [c for c in session.execute.call_args_list
                if "create_campaign_table" in str(c.args[0])]
    assert sp_calls, "SP create_campaign_table nunca fue llamado"
    assert sp_calls[0].args[1] == {"id_camp": CAMPAIGN_ID}


async def test_create_tables_suppresses_sql_notes():
    """SET sql_notes = 0 se ejecuta antes del SP para suprimir warnings."""
    session = _make_session_mock()
    engine, _ = _make_async_engine_mock()
    repo = SmsConfirmRepository(session=session, async_engine=engine)

    await repo.create_campaign_tables([CAMPAIGN_ID])

    sqls = [str(c.args[0]) for c in session.execute.call_args_list]
    assert any("sql_notes" in s for s in sqls)


async def test_empty_df_returns_zero_without_db_call():
    """DataFrame vacío hace cortocircuito antes de cualquier llamada a BD."""
    session = _make_session_mock()
    engine, conn = _make_async_engine_mock()
    repo = SmsConfirmRepository(session=session, async_engine=engine)

    result = await repo.bulk_insert(CAMPAIGN_ID, pl.DataFrame())

    assert result == 0
    conn.execute.assert_not_called()


async def test_bulk_insert_targets_correct_table():
    """INSERT IGNORE apunta a campana_{campaign_id}."""
    session = _make_session_mock()
    engine, conn = _make_async_engine_mock()
    repo = SmsConfirmRepository(session=session, async_engine=engine)

    inserted = await repo.bulk_insert(CAMPAIGN_ID, _sample_df(2))

    assert inserted == 2
    sqls = _sqls(conn)
    inserts = [s for s in sqls if "INSERT" in s.upper()]
    assert inserts, "Ningún INSERT fue ejecutado"
    assert all(f"campana_{CAMPAIGN_ID}" in s for s in inserts)


async def test_bulk_insert_uses_insert_ignore():
    """Se usa INSERT IGNORE para evitar duplicados."""
    session = _make_session_mock()
    engine, conn = _make_async_engine_mock()
    repo = SmsConfirmRepository(session=session, async_engine=engine)

    await repo.bulk_insert(CAMPAIGN_ID, _sample_df(2))

    sqls = _sqls(conn)
    inserts = [s for s in sqls if "INSERT" in s.upper()]
    assert inserts and all("IGNORE" in s.upper() for s in inserts)


async def test_bulk_insert_applies_session_optimizations():
    """SET SESSION unique_checks=0 se ejecuta antes de insertar."""
    session = _make_session_mock()
    engine, conn = _make_async_engine_mock()
    repo = SmsConfirmRepository(session=session, async_engine=engine)

    await repo.bulk_insert(CAMPAIGN_ID, _sample_df(2))

    sqls = _sqls(conn)
    assert any("unique_checks=0" in s for s in sqls)
