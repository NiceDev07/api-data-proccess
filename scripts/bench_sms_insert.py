"""
Benchmark de inserción masiva SMS — comparativa de 3 estrategias.

  A) LOAD DATA LOCAL INFILE  (sync, archivo TSV temporal)
  B) Batch INSERT multi-row  (sync, string SQL manual por chunk)
  C) Parallel async INSERT   (async, chunks concurrentes via asyncmy)

Uso:
    uv run python scripts/bench_sms_insert.py              # 5 000 filas
    uv run python scripts/bench_sms_insert.py --rows 50000
    uv run python scripts/bench_sms_insert.py --keep       # no elimina la tabla
"""

import argparse
import asyncio
import math
import os
import re
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

from config.settings import settings

# ── tabla de prueba ───────────────────────────────────────────────────────────

FAKE_ID    = 99999
TABLE      = f"mail_testing_{FAKE_ID}"
CHUNK_SYNC = 5_000       # filas por chunk en estrategia B
CHUNK_ASYNC = 10_000     # filas por chunk en estrategia C
MAX_CONCURRENCY = 8      # conexiones async simultáneas en estrategia C

_CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE}` (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        celular         VARCHAR(18)  NOT NULL,
        id_campana      INT          NOT NULL DEFAULT {FAKE_ID},
        estado          ENUM('C','F','P','E','B','X','A','D') NOT NULL DEFAULT 'P',
        codigo_corto    VARCHAR(10),
        texto           TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
        identificacion  VARCHAR(250) NOT NULL,
        servicio        VARCHAR(3)   NOT NULL,
        codigo_respuesta VARCHAR(40),
        fecha_envio     DATETIME,
        operador        VARCHAR(40),
        respuesta_operador VARCHAR(20),
        pdu             INT   NOT NULL DEFAULT 0,
        credit          FLOAT NOT NULL DEFAULT 0.0,
        INDEX idx_estado_celular (estado, celular),
        INDEX idx_id_campana     (id_campana)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""
_DROP_SQL   = f"DROP TABLE IF EXISTS `{TABLE}`;"
_TRUNCATE   = f"TRUNCATE TABLE `{TABLE}`;"


# ── datos de prueba ───────────────────────────────────────────────────────────

def make_df(n: int) -> pl.DataFrame:
    import random
    return pl.DataFrame({
        "celular":            [f"57300{random.randint(1_000_000, 9_999_999)}" for _ in range(n)],
        "id_campana":         [FAKE_ID] * n,
        "estado":             ["P"] * n,
        "codigo_corto":       [None] * n,
        "texto":              [f"Mensaje de prueba {i}" for i in range(n)],
        "identificacion":     [""] * n,
        "servicio":           [str((i % 100) + 1) for i in range(n)],
        "codigo_respuesta":   [None] * n,
        "fecha_envio":        [None] * n,
        "operador":           ["CLARO"] * n,
        "respuesta_operador": [None] * n,
        "pdu":                [1] * n,
        "credit":             [0.5] * n,
    })


# ── A: LOAD DATA LOCAL INFILE ─────────────────────────────────────────────────

def insert_load_data(engine, df: pl.DataFrame) -> float:
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tsv")
    os.close(tmp_fd)
    try:
        cols_str = ", ".join(f"`{c}`" for c in df.columns)
        df.write_csv(tmp_path, separator="\t", include_header=False, null_value="")
        safe  = tmp_path.replace("\\", "/")
        sql   = (
            f"LOAD DATA LOCAL INFILE '{safe}' INTO TABLE `{TABLE}` "
            f"CHARACTER SET utf8mb4 "
            f"FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '\"' "
            f"LINES TERMINATED BY '\\n' ({cols_str})"
        )
        t0 = time.perf_counter()
        with engine.begin() as conn:
            conn.execute(text(sql))
        return time.perf_counter() - t0
    finally:
        os.unlink(tmp_path)


# ── B: Batch INSERT multi-row (sync) ──────────────────────────────────────────

def insert_batch(engine, df: pl.DataFrame) -> float:
    rows     = df.rows()
    cols_str = ", ".join(f"`{c}`" for c in df.columns)

    t0 = time.perf_counter()
    with engine.begin() as conn:
        conn.execute(text("SET SESSION unique_checks=0, foreign_key_checks=0"))
        for i in range(0, len(rows), CHUNK_SYNC):
            chunk        = rows[i : i + CHUNK_SYNC]
            placeholders = ", ".join(
                "(" + ", ".join(
                    "NULL" if v is None
                    else f"'{str(v).replace(chr(39), chr(39)*2)}'"
                    for v in row
                ) + ")"
                for row in chunk
            )
            conn.execute(text(
                f"INSERT IGNORE INTO `{TABLE}` ({cols_str}) VALUES {placeholders}"
            ))
        conn.execute(text("SET SESSION unique_checks=1, foreign_key_checks=1"))
    return time.perf_counter() - t0


# ── C: Parallel async INSERT (asyncmy) ───────────────────────────────────────

async def insert_parallel_async(async_engine, df: pl.DataFrame) -> float:
    cols     = df.columns
    cols_sql = ", ".join(f"`{c}`" for c in cols)
    binds    = ", ".join(f":{c}" for c in cols)
    stmt     = text(f"INSERT IGNORE INTO `{TABLE}` ({cols_sql}) VALUES ({binds})")
    tune     = text("SET SESSION unique_checks=0, foreign_key_checks=0")

    n_chunks    = math.ceil(df.height / CHUNK_ASYNC)
    concurrency = min(n_chunks, MAX_CONCURRENCY)
    sem         = asyncio.Semaphore(concurrency)

    async def insert_chunk(offset: int) -> None:
        rows = df.slice(offset, CHUNK_ASYNC).to_dicts()
        async with sem:
            async with async_engine.begin() as conn:
                await conn.execute(tune)
                await conn.execute(stmt, rows)

    t0    = time.perf_counter()
    tasks = [asyncio.create_task(insert_chunk(i * CHUNK_ASYNC)) for i in range(n_chunks)]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for t in tasks:
            if not t.done():
                t.cancel()
        raise
    return time.perf_counter() - t0


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Benchmark inserción SMS (3 estrategias)")
    parser.add_argument("--rows", type=int, default=5_000)
    parser.add_argument("--keep", action="store_true", help="No eliminar la tabla al finalizar")
    args = parser.parse_args()

    sync_dsn  = re.sub(r"^mysql\+\w+://", "mysql+pymysql://", settings.DB_TELEFONOS_CAMPANAS)
    async_dsn = re.sub(r"^mysql\+\w+://", "mysql+asyncmy://", settings.DB_TELEFONOS_CAMPANAS)

    sync_engine  = create_engine(
        sync_dsn, pool_pre_ping=True,
        connect_args={"local_infile": True, "charset": "utf8mb4"},
    )
    async_engine = create_async_engine(async_dsn, pool_pre_ping=True, pool_size=MAX_CONCURRENCY)

    n = args.rows
    print(f"\n{'='*60}")
    print(f"  Benchmark SMS insert | tabla: {TABLE} | filas: {n:,}")
    print(f"{'='*60}")

    # crear tabla
    with sync_engine.begin() as conn:
        conn.execute(text(_DROP_SQL))
        conn.execute(text(_CREATE_SQL))
    print(f"  Tabla '{TABLE}' creada.\n")

    df = make_df(n)

    # A — LOAD DATA
    t_ld = insert_load_data(sync_engine, df)
    print(f"  A) LOAD DATA LOCAL INFILE : {t_ld:.3f}s  ({n/t_ld:>10,.0f} filas/s)")
    with sync_engine.begin() as conn:
        conn.execute(text(_TRUNCATE))

    # B — Batch INSERT sync
    t_bi = insert_batch(sync_engine, df)
    print(f"  B) Batch INSERT sync      : {t_bi:.3f}s  ({n/t_bi:>10,.0f} filas/s)")
    with sync_engine.begin() as conn:
        conn.execute(text(_TRUNCATE))

    # C — Parallel async
    t_pa = asyncio.run(insert_parallel_async(async_engine, df))
    print(f"  C) Parallel async INSERT  : {t_pa:.3f}s  ({n/t_pa:>10,.0f} filas/s)")

    best = min(t_ld, t_bi, t_pa)
    print(f"\n  Ganancia vs LOAD DATA  : B={t_ld/t_bi:.2f}x  C={t_ld/t_pa:.2f}x")
    print(f"  Ganancia vs Batch sync : A={t_bi/t_ld:.2f}x  C={t_bi/t_pa:.2f}x")
    winner = ["A) LOAD DATA", "B) Batch sync", "C) Parallel async"][
        [t_ld, t_bi, t_pa].index(best)
    ]
    print(f"\n  ✓ Ganador: {winner}  ({n/best:,.0f} filas/s)\n")

    if not args.keep:
        with sync_engine.begin() as conn:
            conn.execute(text(_DROP_SQL))
        print(f"  Tabla '{TABLE}' eliminada.")

    asyncio.run(async_engine.dispose())
    sync_engine.dispose()
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
