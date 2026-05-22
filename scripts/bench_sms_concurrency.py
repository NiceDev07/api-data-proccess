"""
Benchmark de concurrencia — Parallel async INSERT (C) vs LOAD DATA LOCAL INFILE (A)

Objetivo: determinar si la estrategia C mantiene ventaja bajo carga concurrente real
o si LOAD DATA supera a C cuando múltiples confirms corren simultáneamente.

Prueba principal:
  - 700,000 registros por campaña
  - 1, 2 y 4 confirms simultáneos (cada uno a su propia tabla campana_{id})
  - Ambas estrategias bajo las mismas condiciones

Uso:
    uv run python scripts/bench_sms_concurrency.py
    uv run python scripts/bench_sms_concurrency.py --rows 700000
    uv run python scripts/bench_sms_concurrency.py --quick   # solo 4 confirms simultáneos
"""

import argparse
import asyncio
import math
import os
import re
import sys
import tempfile
import time
from dataclasses import dataclass, field

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

from config.settings import settings
from modules.process.infrastructure.repositories.sms_confirm import _CREATE_TABLE_SQL

# ── configuración ─────────────────────────────────────────────────────────────

CAMPAIGN_IDS    = [99901, 99902, 99903, 99904]
CHUNK_ASYNC     = 20_000
MAX_CONCURRENCY = 12

_ASYNC_DSN = re.sub(r"^mysql\+\w+://", "mysql+asyncmy://", settings.DB_TELEFONOS_CAMPANAS)
_SYNC_DSN  = re.sub(r"^mysql\+\w+://", "mysql+pymysql://", settings.DB_TELEFONOS_CAMPANAS)


# ── modelos de resultado ───────────────────────────────────────────────────────

@dataclass
class CampaignResult:
    campaign_id:          int
    insert_time_seconds:  float
    rows_inserted:        int
    error:                str | None = None

@dataclass
class BenchmarkResult:
    strategy:             str
    records_per_campaign: int
    parallel_confirms:    int
    total_records:        int
    total_time_seconds:   float
    rows_per_second:      float
    errors:               list[str]
    campaigns:            list[CampaignResult] = field(default_factory=list)


# ── datos de prueba ────────────────────────────────────────────────────────────

def make_df(n: int) -> pl.DataFrame:
    import random
    return pl.DataFrame({
        "celular":            [f"57300{random.randint(1_000_000, 9_999_999)}" for _ in range(n)],
        "id_campana":         [0] * n,          # se sobreescribe por campaña en LOAD DATA
        "estado":             ["P"] * n,
        "codigo_corto":       [None] * n,
        "texto":              [f"Hola, este es tu mensaje de campaña #{i}" for i in range(n)],
        "identificacion":     [""] * n,
        "servicio":           [str((i % 100) + 1) for i in range(n)],
        "codigo_respuesta":   [None] * n,
        "fecha_envio":        [None] * n,
        "operador":           ["CLARO"] * n,
        "respuesta_operador": [None] * n,
        "pdu":                [1] * n,
        "credit":             [0.5] * n,
    })


# ── setup / teardown ──────────────────────────────────────────────────────────

def setup_tables(sync_engine) -> None:
    with sync_engine.begin() as conn:
        conn.execute(text("SET sql_notes = 0"))
        for cid in CAMPAIGN_IDS:
            conn.execute(text(f"DROP TABLE IF EXISTS `campana_{cid}`"))
            conn.execute(text(_CREATE_TABLE_SQL.format(campaign_id=cid)))
        conn.execute(text("SET sql_notes = 1"))

def truncate_tables(sync_engine) -> None:
    with sync_engine.begin() as conn:
        for cid in CAMPAIGN_IDS:
            conn.execute(text(f"TRUNCATE TABLE `campana_{cid}`"))

def drop_tables(sync_engine) -> None:
    with sync_engine.begin() as conn:
        for cid in CAMPAIGN_IDS:
            conn.execute(text(f"DROP TABLE IF EXISTS `campana_{cid}`"))


# ── estrategia C: Parallel async INSERT ───────────────────────────────────────

async def confirm_async(campaign_id: int, df: pl.DataFrame, async_engine) -> CampaignResult:
    df = df.with_columns(pl.lit(campaign_id).alias("id_campana"))
    cols     = df.columns
    cols_sql = ", ".join(f"`{c}`" for c in cols)
    binds    = ", ".join(f":{c}" for c in cols)
    stmt     = text(f"INSERT IGNORE INTO `campana_{campaign_id}` ({cols_sql}) VALUES ({binds})")

    n_chunks    = math.ceil(df.height / CHUNK_ASYNC)
    concurrency = min(n_chunks, MAX_CONCURRENCY)
    sem         = asyncio.Semaphore(concurrency)

    errors: list[str] = []

    async def _insert_chunk(offset: int) -> None:
        rows = df.slice(offset, CHUNK_ASYNC).to_dicts()
        async with sem:
            async with async_engine.begin() as conn:
                await conn.execute(text("SET SESSION unique_checks=0, foreign_key_checks=0"))
                await conn.execute(stmt, rows)
                await conn.execute(text("SET SESSION unique_checks=1, foreign_key_checks=1"))

    t0    = time.perf_counter()
    tasks = [asyncio.create_task(_insert_chunk(i * CHUNK_ASYNC)) for i in range(n_chunks)]
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        for t in tasks:
            if not t.done():
                t.cancel()
        errors.append(str(e))

    elapsed = time.perf_counter() - t0
    return CampaignResult(
        campaign_id=campaign_id,
        insert_time_seconds=round(elapsed, 3),
        rows_inserted=df.height if not errors else 0,
        error=errors[0] if errors else None,
    )


# ── estrategia A: LOAD DATA LOCAL INFILE ──────────────────────────────────────

def _sync_load_data(campaign_id: int, df: pl.DataFrame, sync_engine) -> CampaignResult:
    df = df.with_columns(pl.lit(campaign_id).alias("id_campana"))
    cols_str = ", ".join(f"`{c}`" for c in df.columns)
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=f"_camp{campaign_id}.tsv")
    os.close(tmp_fd)
    try:
        df.write_csv(tmp_path, separator="\t", include_header=False, null_value="")
        safe = tmp_path.replace("\\", "/")
        sql  = (
            f"LOAD DATA LOCAL INFILE '{safe}' "
            f"INTO TABLE `campana_{campaign_id}` "
            f"CHARACTER SET utf8mb4 "
            f"FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '\"' "
            f"LINES TERMINATED BY '\\n' ({cols_str})"
        )
        t0 = time.perf_counter()
        with sync_engine.begin() as conn:
            conn.execute(text(sql))
        elapsed = time.perf_counter() - t0
        return CampaignResult(
            campaign_id=campaign_id,
            insert_time_seconds=round(elapsed, 3),
            rows_inserted=df.height,
        )
    except Exception as e:
        return CampaignResult(
            campaign_id=campaign_id,
            insert_time_seconds=0.0,
            rows_inserted=0,
            error=str(e),
        )
    finally:
        os.unlink(tmp_path)

async def confirm_load_data(campaign_id: int, df: pl.DataFrame, sync_engine) -> CampaignResult:
    return await asyncio.to_thread(_sync_load_data, campaign_id, df, sync_engine)


# ── runner de concurrencia ────────────────────────────────────────────────────

async def run_concurrent(
    strategy:    str,
    n_confirms:  int,
    df:          pl.DataFrame,
    async_engine,
    sync_engine,
) -> BenchmarkResult:
    campaign_ids = CAMPAIGN_IDS[:n_confirms]

    if strategy == "async":
        coros = [confirm_async(cid, df, async_engine) for cid in campaign_ids]
    else:
        coros = [confirm_load_data(cid, df, sync_engine) for cid in campaign_ids]

    t0      = time.perf_counter()
    results = await asyncio.gather(*coros, return_exceptions=True)
    elapsed = time.perf_counter() - t0

    campaign_results: list[CampaignResult] = []
    errors: list[str] = []
    total_rows = 0

    for r in results:
        if isinstance(r, Exception):
            errors.append(str(r))
        else:
            campaign_results.append(r)
            total_rows += r.rows_inserted
            if r.error:
                errors.append(r.error)

    return BenchmarkResult(
        strategy=strategy,
        records_per_campaign=df.height,
        parallel_confirms=n_confirms,
        total_records=total_rows,
        total_time_seconds=round(elapsed, 3),
        rows_per_second=round(total_rows / elapsed) if elapsed > 0 else 0,
        errors=errors,
        campaigns=campaign_results,
    )


# ── reporte ───────────────────────────────────────────────────────────────────

def print_result(r: BenchmarkResult) -> None:
    label = "C) Parallel async INSERT" if r.strategy == "async" else "A) LOAD DATA LOCAL INFILE"
    status = "ERROR" if r.errors else "OK"
    print(f"\n  [{status}] {label}")
    print(f"       Confirms simultáneos : {r.parallel_confirms}")
    print(f"       Registros por campaña: {r.records_per_campaign:,}")
    print(f"       Total registros      : {r.total_records:,}")
    print(f"       Tiempo total         : {r.total_time_seconds:.3f}s")
    print(f"       Throughput total     : {r.rows_per_second:,.0f} registros/s")
    print(f"       Desglose por campaña :")
    for cr in r.campaigns:
        tag = f"  ERROR: {cr.error}" if cr.error else ""
        print(f"         campana_{cr.campaign_id}: {cr.insert_time_seconds:.3f}s  —  {cr.rows_inserted:,} registros{tag}")
    if r.errors:
        for e in r.errors:
            print(f"       ⚠ {e}")


def print_comparison(results: list[BenchmarkResult]) -> None:
    print(f"\n{'─'*62}")
    print(f"  {'Estrategia':<26} {'Confirms':>8}  {'Tiempo':>8}  {'Reg/s':>10}")
    print(f"{'─'*62}")
    for r in results:
        label = "C) Parallel async" if r.strategy == "async" else "A) LOAD DATA"
        print(f"  {label:<26} {r.parallel_confirms:>8}  {r.total_time_seconds:>7.3f}s  {r.rows_per_second:>10,.0f}")
    print(f"{'─'*62}")

    # ganador por nivel de concurrencia
    concurrency_levels = sorted({r.parallel_confirms for r in results})
    print(f"\n  Ganador por nivel de concurrencia:")
    for level in concurrency_levels:
        group = [r for r in results if r.parallel_confirms == level]
        if len(group) == 2:
            winner = min(group, key=lambda x: x.total_time_seconds)
            loser  = max(group, key=lambda x: x.total_time_seconds)
            pct    = round((loser.total_time_seconds / winner.total_time_seconds - 1) * 100, 1)
            wlabel = "C) Parallel async" if winner.strategy == "async" else "A) LOAD DATA"
            print(f"    {level} confirm(s): {wlabel}  (+{pct}% más rápido)")


# ── main ──────────────────────────────────────────────────────────────────────

async def main_async(rows: int, quick: bool) -> None:
    async_engine = create_async_engine(
        _ASYNC_DSN, pool_pre_ping=True,
        pool_size=MAX_CONCURRENCY * len(CAMPAIGN_IDS),  # para soportar confirms simultáneos
        max_overflow=8,
    )
    sync_engine = create_engine(
        _SYNC_DSN, pool_pre_ping=True,
        pool_size=len(CAMPAIGN_IDS),
        max_overflow=4,
        connect_args={"local_infile": True, "charset": "utf8mb4"},
    )

    print(f"\n{'='*62}")
    print(f"  Benchmark de concurrencia SMS")
    print(f"  Estrategias : C) Parallel async INSERT  vs  A) LOAD DATA")
    print(f"  Registros   : {rows:,} por campaña")
    print(f"  Config C    : chunk={CHUNK_ASYNC:,} / concurrencia={MAX_CONCURRENCY}")
    print(f"{'='*62}")

    print(f"\n  Generando DataFrame de {rows:,} registros...")
    df = make_df(rows)

    print(f"  Creando tablas de prueba: {['campana_'+str(c) for c in CAMPAIGN_IDS]}...")
    setup_tables(sync_engine)

    concurrency_levels = [4] if quick else [1, 2, 4]
    all_results: list[BenchmarkResult] = []

    for n_confirms in concurrency_levels:
        print(f"\n{'─'*62}")
        print(f"  PRUEBA: {n_confirms} confirm(s) simultáneo(s) — {rows*n_confirms:,} registros totales")
        print(f"{'─'*62}")

        # C — Parallel async
        truncate_tables(sync_engine)
        print(f"\n  Ejecutando C) Parallel async x{n_confirms}...")
        result_c = await run_concurrent("async", n_confirms, df, async_engine, sync_engine)
        print_result(result_c)
        all_results.append(result_c)

        # A — LOAD DATA
        truncate_tables(sync_engine)
        print(f"\n  Ejecutando A) LOAD DATA x{n_confirms}...")
        result_a = await run_concurrent("load_data", n_confirms, df, async_engine, sync_engine)
        print_result(result_a)
        all_results.append(result_a)

    # comparativa final
    print(f"\n\n{'='*62}")
    print(f"  COMPARATIVA FINAL — {rows:,} registros por campaña")
    print(f"{'='*62}")
    print_comparison(all_results)

    # decisión
    results_4 = [r for r in all_results if r.parallel_confirms == 4]
    if len(results_4) == 2:
        c_result  = next(r for r in results_4 if r.strategy == "async")
        ld_result = next(r for r in results_4 if r.strategy == "load_data")
        print(f"\n  DECISIÓN:")
        if c_result.total_time_seconds < ld_result.total_time_seconds:
            print(f"  C sigue ganando con 4 confirms simultáneos.")
            print(f"  → Mantener estrategia C. Optimizar con workers/queue si es necesario.")
        else:
            diff = round((c_result.total_time_seconds / ld_result.total_time_seconds - 1) * 100, 1)
            print(f"  LOAD DATA supera a C en {diff}% con 4 confirms simultáneos.")
            print(f"  → Implementar estrategia híbrida:")
            print(f"      ≤ 50,000 registros  →  C) Parallel async INSERT")
            print(f"      > 50,000 registros  →  A) LOAD DATA LOCAL INFILE")

    print(f"\n  Eliminando tablas de prueba...")
    drop_tables(sync_engine)
    await async_engine.dispose()
    sync_engine.dispose()
    print(f"{'='*62}\n")


def main():
    parser = argparse.ArgumentParser(description="Benchmark de concurrencia SMS")
    parser.add_argument("--rows",  type=int, default=700_000, help="Registros por campaña")
    parser.add_argument("--quick", action="store_true", help="Solo prueba con 4 confirms simultáneos")
    args = parser.parse_args()
    asyncio.run(main_async(args.rows, args.quick))


if __name__ == "__main__":
    main()
