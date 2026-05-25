"""
Tests de integración — EmailProcessor end-to-end.

El DataFrame de entrada se construye inline (no requiere archivo CSV).
Los servicios externos (cost, exclusión) se mockean.
"""
from pathlib import Path

import polars as pl
import pytest

from modules.process.app.process.email import EmailProcessor
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason

from modules.process.test.conftest import (
    AnalysisStorage,
    base_email_df,
    email_cost_mock,
    email_exclusion_mock,
    make_ctx,
    make_excl_config,
    save_summary,
)

pytestmark = pytest.mark.anyio

# ── helpers locales ───────────────────────────────────────────────────────────

def make_processor(scenario: str, cost: float | None = None, excl_emails=None) -> tuple:
    storage = AnalysisStorage(f"email_flow/{scenario}")
    processor = EmailProcessor(
        exclusion_source=email_exclusion_mock(excl_emails, col="email"),
        cost_service=email_cost_mock(cost),
        storage=storage,
    )
    return processor, storage


# ── tests ─────────────────────────────────────────────────────────────────────

async def test_email_happy_path():
    """Todos los emails válidos → credits calculados, parquet guardado."""
    processor, storage = make_processor("happy_path")
    ctx = make_ctx(
        content="Tu código es {code}.",
        demographic="email",
        sub_service="standard",
        subject="Asunto de prueba",
    )
    df = pl.DataFrame({
        "email": ["user@gmail.com", "client@hotmail.com"],
        "code":  ["ABC1",           "XYZ2"],
    })
    result = await processor.process(df, ctx)
    save_summary("email_flow/happy_path", result)

    assert result["success"] is True
    sg = result["summaryGeneral"]
    assert sg["total_records"] == 2
    assert sg["total_excluded"] == 0
    assert sg["total_credits"] > 0


async def test_email_invalid_format_excluded():
    """Emails con formato inválido quedan fuera; resumen refleja exclusión."""
    processor, storage = make_processor("invalid_email")
    ctx = make_ctx(content="Hola", demographic="email", sub_service="standard", subject="Asunto de prueba")
    df = pl.DataFrame({"email": ["invalid", "valid@gmail.com"]})
    result = await processor.process(df, ctx)
    save_summary("email_flow/invalid_email", result)

    sg = result["summaryGeneral"]
    assert sg["total_records"] == 1
    assert sg["total_excluded"] == 1

    saved = storage.last_df()
    nok = saved.filter(~pl.col(Cols.is_ok))
    assert nok[Cols.error_code][0] == ExclusionReason.INVALID_EMAIL


async def test_email_exclusion_list():
    """Email en lista de exclusión → marcado y no contabilizado en créditos."""
    processor, storage = make_processor("exclusion", excl_emails=["blocked@gmail.com"])
    excl_cfg = make_excl_config(demographic="email")
    ctx = make_ctx(
        content="Oferta especial",
        demographic="email",
        sub_service="standard",
        use_exclusion=True,
        excl_config=excl_cfg,
        subject="Asunto de prueba",
    )
    df = pl.DataFrame({"email": ["blocked@gmail.com", "free@hotmail.com"]})
    result = await processor.process(df, ctx)
    save_summary("email_flow/exclusion", result)

    sg = result["summaryGeneral"]
    assert sg["total_records"] == 1
    assert sg["total_excluded"] == 1

    saved = storage.last_df()
    nok = saved.filter(~pl.col(Cols.is_ok))
    assert nok[Cols.error_code][0] == ExclusionReason.EXCLUSION_LIST


async def test_email_summary_groups_by_domain():
    """Dos dominios distintos producen dos grupos en summaryGroup."""
    processor, storage = make_processor("domains", cost=0.02)
    ctx = make_ctx(content="Mensaje", demographic="email", sub_service="standard", subject="Asunto de prueba")
    df = pl.DataFrame({"email": ["a@gmail.com", "b@hotmail.com"]})
    result = await processor.process(df, ctx)
    save_summary("email_flow/domains", result)

    groups = result["summaryGroup"]
    domains = {g["domain"] for g in groups}
    assert "gmail.com" in domains
    assert "hotmail.com" in domains


async def test_email_unknown_domain_grouped_as_others():
    """Dominio desconocido se agrupa como 'others' en el resumen."""
    processor, storage = make_processor("unknown_domain")
    ctx = make_ctx(content="Mensaje", demographic="email", sub_service="standard", subject="Asunto de prueba")
    df = pl.DataFrame({"email": ["user@empresa-privada.com"]})
    result = await processor.process(df, ctx)
    save_summary("email_flow/unknown_domain", result)

    groups = result["summaryGroup"]
    assert any(g["domain"] == "others" for g in groups)


async def test_email_parquet_contains_domain_column():
    """El archivo parquet guardado debe incluir __EMAIL_DOMAIN__."""
    processor, storage = make_processor("parquet_domain")
    ctx = make_ctx(content="Hola", demographic="email", sub_service="standard", subject="Asunto de prueba")
    df = pl.DataFrame({"email": ["user@gmail.com"]})
    await processor.process(df, ctx)

    parquet_path = Path(storage.saved_paths[0])
    loaded = pl.read_parquet(parquet_path)
    assert Cols.email_domain in loaded.columns
    assert loaded[Cols.email_domain][0] == "gmail.com"


async def test_email_parquet_audit_columns():
    """is_ok y error_code deben estar en el parquet guardado."""
    processor, storage = make_processor("parquet_audit")
    ctx = make_ctx(content="Hola", demographic="email", sub_service="standard", subject="Asunto de prueba")
    df = pl.DataFrame({"email": ["user@gmail.com", "badformat"]})
    await processor.process(df, ctx)

    loaded = pl.read_parquet(storage.saved_paths[0])
    assert Cols.is_ok in loaded.columns
    assert Cols.error_code in loaded.columns
    valid = loaded.filter(pl.col(Cols.is_ok))
    invalid = loaded.filter(~pl.col(Cols.is_ok))
    assert len(valid) == 1
    assert len(invalid) == 1


async def test_email_all_excluded_returns_zero_credits():
    """Si todos los emails son inválidos, total_credits debe ser 0."""
    processor, storage = make_processor("all_excluded")
    ctx = make_ctx(content="Hola", demographic="email", sub_service="standard", subject="Asunto de prueba")
    df = pl.DataFrame({"email": ["invalid1", "invalid2", "invalid3"]})
    result = await processor.process(df, ctx)

    sg = result["summaryGeneral"]
    assert sg["total_records"] == 0
    assert sg["total_excluded"] == 3
    assert sg["total_credits"] == pytest.approx(0.0)


async def test_email_cost_catchall_applied_to_unknown_domain():
    """El costo plano se aplica a cualquier dominio."""
    processor, storage = make_processor("catchall", cost=0.01)
    ctx = make_ctx(content="Hola", demographic="email", sub_service="standard", subject="Asunto de prueba")
    df = pl.DataFrame({"email": ["user@empresa.io"]})
    result = await processor.process(df, ctx)

    sg = result["summaryGeneral"]
    assert sg["total_credits"] == pytest.approx(0.01)
