"""
Tests de integración — CallBlastingProcessor end-to-end.

El DataFrame de entrada se construye inline (no requiere archivo CSV).
Los servicios externos (numeration, cost, exclusion, duration) se mockean.
"""
from pathlib import Path

import polars as pl
import pytest

from modules.process.app.process.callblasting import CallBlastingProcessor
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason

from .conftest import (
    AnalysisStorage,
    cb_cost_mock,
    duration_mock,
    exclusion_mock,
    make_ctx,
    make_excl_config,
    numeration_mock,
    save_summary,
)

pytestmark = pytest.mark.anyio

# ── helpers locales ───────────────────────────────────────────────────────────

_DEFAULT_NUMBERS = ["3005973563", "3208392650"]

_COST_ROWS = [("57", 0.06, "COLOMBIA", 30.0, 15.0)]


def make_processor(
    scenario: str,
    cost_rows=None,
    excl_numbers=None,
    duration_secs: float = 30.0,
) -> tuple:
    storage = AnalysisStorage(f"callblasting_flow/{scenario}")
    processor = CallBlastingProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(excl_numbers, col="number"),
        cost_service=cb_cost_mock(cost_rows or _COST_ROWS),
        storage=storage,
        duration_provider=duration_mock(duration_secs),
    )
    return processor, storage


def base_df(numbers: list[str] | None = None) -> pl.DataFrame:
    nums = numbers or _DEFAULT_NUMBERS
    return pl.DataFrame({"number": nums})


def cb_ctx(**kwargs):
    """make_ctx con demographic='number' por defecto para todos los tests CB."""
    kwargs.setdefault("demographic", "number")
    return make_ctx(**kwargs)


# ── tests: standard ──────────────────────────────────────────────────────────

async def test_cb_standard_happy_path():
    """Dos registros válidos con audioDuration → créditos calculados y parquet guardado."""
    processor, storage = make_processor("standard_happy_path")
    ctx = cb_ctx(content="", sub_service="standard", audio_duration=30.0)
    result = await processor.process(base_df(), ctx)
    save_summary("callblasting_flow/standard_happy_path", result)

    assert result["success"] is True
    sg = result["summaryGeneral"]
    assert sg["total_records"] == 2
    assert sg["total_excluded"] == 0
    assert sg["total_seconds"] > 0
    assert sg["total_credits"] > 0


async def test_cb_standard_audio_path_uses_provider():
    """audioPath sin audioDuration → duración resuelta vía duration_provider."""
    processor, storage = make_processor("standard_audio_path", duration_secs=45.0)
    ctx = cb_ctx(content="", sub_service="standard", audio_path="/tmp/audio.mp3")
    result = await processor.process(base_df(), ctx)
    save_summary("callblasting_flow/standard_audio_path", result)

    assert result["success"] is True
    assert result["summaryGeneral"]["total_records"] == 2

    saved = storage.last_df()
    # duration_mock returns 45.0 → ceil(45) + OPERATION_MARGIN_SECS(5) = 50
    assert (saved.filter(pl.col(Cols.is_ok))[Cols.seconds] == 50).all()


async def test_cb_standard_no_duration_raises():
    """Sin audioDuration ni audioPath → ValueError antes de guardar."""
    processor, _ = make_processor("standard_no_duration")
    ctx = cb_ctx(content="", sub_service="standard")
    with pytest.raises(ValueError, match="audioDuration"):
        await processor.process(base_df(), ctx)


async def test_cb_standard_parquet_columns():
    """El parquet estándar debe contener columnas de salida + auditoría; sin message."""
    processor, storage = make_processor("standard_parquet")
    ctx = cb_ctx(content="", sub_service="standard", audio_duration=20.0)
    await processor.process(base_df(), ctx)

    loaded = pl.read_parquet(storage.saved_paths[0])
    for col in (Cols.number_concat, Cols.number_operator, Cols.seconds, Cols.credits,
                Cols.is_ok, Cols.error_code):
        assert col in loaded.columns
    assert Cols.message not in loaded.columns


async def test_cb_standard_summary_group_by_operator():
    """Un único prefijo de costo → un único grupo en summaryGroup."""
    processor, _ = make_processor("standard_groups")
    ctx = cb_ctx(content="", sub_service="standard", audio_duration=30.0)
    result = await processor.process(base_df(), ctx)

    groups = result["summaryGroup"]
    assert len(groups) == 1
    assert groups[0]["operator"] == "COLOMBIA"
    assert groups[0]["total"] == 2


async def test_cb_standard_credits_calculation():
    """Fórmula: cycles=initial cuando seconds<=initial → créditos esperados."""
    # cost=0.06, initial=30, incremental=15 → cost_per_second=0.001
    # audio_duration=20 → seconds=ceil(20)+5=25 ≤ initial(30)
    # cycles=initial=30, credits=30*15*0.001=0.45 por registro
    processor, storage = make_processor("standard_credits")
    ctx = cb_ctx(content="", sub_service="standard", audio_duration=20.0)
    result = await processor.process(base_df(), ctx)

    assert result["summaryGeneral"]["total_credits"] == pytest.approx(0.45 * 2, abs=1e-3)
    valid = storage.last_df().filter(pl.col(Cols.is_ok))
    for c in valid[Cols.credits].to_list():
        assert c == pytest.approx(0.45, abs=1e-3)


# ── tests: custom ─────────────────────────────────────────────────────────────

async def test_cb_custom_happy_path():
    """Dos registros con mensaje personalizado → segundos y créditos calculados."""
    processor, storage = make_processor("custom_happy_path")
    ctx = cb_ctx(content="Hola {nombre}, su pedido llegó.", sub_service="custom")
    df = pl.DataFrame({"number": ["3005973563", "3208392650"], "nombre": ["Ana", "Luis"]})
    result = await processor.process(df, ctx)
    save_summary("callblasting_flow/custom_happy_path", result)

    assert result["success"] is True
    sg = result["summaryGeneral"]
    assert sg["total_records"] == 2
    assert sg["total_excluded"] == 0
    assert sg["total_credits"] > 0


async def test_cb_custom_parquet_has_message_column():
    """El parquet custom debe incluir la columna de mensaje."""
    processor, storage = make_processor("custom_parquet")
    ctx = cb_ctx(content="Hola {nombre}.", sub_service="custom")
    df = pl.DataFrame({"number": ["3005973563", "3208392650"], "nombre": ["Pedro", "María"]})
    await processor.process(df, ctx)

    loaded = pl.read_parquet(storage.saved_paths[0])
    for col in (Cols.message, Cols.seconds, Cols.credits):
        assert col in loaded.columns


async def test_cb_custom_per_record_seconds():
    """Mensajes de distintas longitudes producen distintos segundos por registro."""
    processor, storage = make_processor("custom_per_record")
    ctx = cb_ctx(content="{texto}", sub_service="custom")
    df = pl.DataFrame({
        "number": ["3005973563", "3208392650"],
        "texto": [
            "Hola",
            "Este es un mensaje mucho más largo con muchas más palabras para que dure más tiempo",
        ],
    })
    await processor.process(df, ctx)

    valid = storage.last_df().filter(pl.col(Cols.is_ok))
    secs = valid[Cols.seconds].to_list()
    assert secs[1] > secs[0]


# ── tests: exclusión y operador ───────────────────────────────────────────────

async def test_cb_exclusion_list():
    """Número en lista de exclusión → marcado y no contabilizado en créditos."""
    excluded = 3005973563
    processor, storage = make_processor("exclusion", excl_numbers=[excluded])
    excl_cfg = make_excl_config(demographic="number")
    ctx = cb_ctx(
        content="",
        sub_service="standard",
        audio_duration=30.0,
        use_exclusion=True,
        excl_config=excl_cfg,
    )
    result = await processor.process(base_df(), ctx)
    save_summary("callblasting_flow/exclusion", result)

    sg = result["summaryGeneral"]
    assert sg["total_records"] == 1
    assert sg["total_excluded"] == 1

    nok = storage.last_df().filter(~pl.col(Cols.is_ok))
    assert nok[Cols.error_code][0] == ExclusionReason.EXCLUSION_LIST


async def test_cb_no_operator_excluded():
    """Número fuera de todos los rangos → excluido con NO_OPERATOR."""
    processor_partial = CallBlastingProcessor(
        numeration_service=numeration_mock(
            starts=[3000000000], ends=[3099999999], operators=["CLARO"]
        ),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cb_cost_mock(_COST_ROWS),
        storage=AnalysisStorage("callblasting_flow/no_operator"),
        duration_provider=duration_mock(),
    )
    ctx = cb_ctx(content="", sub_service="standard", audio_duration=30.0)
    result = await processor_partial.process(base_df(), ctx)
    save_summary("callblasting_flow/no_operator", result)

    sg = result["summaryGeneral"]
    assert sg["total_records"] == 1
    assert sg["total_excluded"] == 1


async def test_cb_invalid_sub_service_raises():
    """Sub-servicio desconocido → ValueError."""
    processor, _ = make_processor("invalid_subservice")
    ctx = cb_ctx(content="", sub_service="informative")  # SMS sub-service → inválido en CB
    with pytest.raises(ValueError, match="Sub-servicio"):
        await processor.process(base_df(), ctx)


async def test_cb_all_excluded_zero_credits():
    """Todos los registros excluidos → total_credits y total_seconds == 0."""
    processor_all_excl = CallBlastingProcessor(
        numeration_service=numeration_mock(
            starts=[9000000000], ends=[9099999999], operators=["X"]
        ),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cb_cost_mock(_COST_ROWS),
        storage=AnalysisStorage("callblasting_flow/all_excluded"),
        duration_provider=duration_mock(),
    )
    ctx = cb_ctx(content="", sub_service="standard", audio_duration=30.0)
    result = await processor_all_excl.process(base_df(), ctx)
    save_summary("callblasting_flow/all_excluded", result)

    sg = result["summaryGeneral"]
    assert sg["total_records"] == 0
    assert sg["total_excluded"] == 2
    assert sg["total_credits"] == pytest.approx(0.0)
    assert sg["total_seconds"] == 0
