"""
Bulk insert performance test.
Multiplies data.csv records to reach different row counts and measures insertion time.

Usage:
    uv run python test_bulk_insert.py
"""
import re
import sys
import time

import polars as pl
from sqlalchemy import create_engine, text, insert, Table, Column, Integer, String, Text, Float, Enum, MetaData

sys.path.insert(0, "src")
from config.settings import settings

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
CAMPAIGN_ID   = 999999          # Disposable test table: campana_999999
THRESHOLD     = 50_000          # Must match repository _LOAD_DATA_THRESHOLD
INSERT_CHUNK  = 5_000
ROW_COUNTS    = [100, 1_000, 10_000, 50_000, 100_000, 500_000, 1_000_000]

SYNC_DSN = re.sub(r"^mysql\+\w+://", "mysql+pymysql://", settings.db_telefonos_campanas)

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def make_engine():
    return create_engine(SYNC_DSN, connect_args={"local_infile": True})


def build_dataframe(n_rows: int) -> pl.DataFrame:
    base = pl.DataFrame({
        "celular":        ["TEST-ABC123", "TEST-XYZ789"],
        "texto":          ["Hola, este es un mensaje de prueba 😊", "Hola, este es otro mensaje de prueba"],
        "operador":       ["CLARO", "MOVISTAR"],
        "pdu":            [1, 1],
        "credit":         [1.0, 1.0],
        "identificacion": ["", ""],
        "estado":         ["P", "X"],
        "servicio":       ["1", "2"],
        "id_campana":     [CAMPAIGN_ID, CAMPAIGN_ID],
    })
    repeats = (n_rows // base.height) + 1
    return pl.concat([base] * repeats).head(n_rows)


def create_table(engine):
    with engine.begin() as conn:
        conn.execute(text("SET sql_notes = 0"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `campana_{CAMPAIGN_ID}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                celular VARCHAR(18) NOT NULL,
                id_campana INT NOT NULL DEFAULT '{CAMPAIGN_ID}',
                estado ENUM('C','F','P','E','B','X','A','D') NOT NULL DEFAULT 'P',
                codigo_corto VARCHAR(10),
                texto TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                identificacion VARCHAR(250) NOT NULL,
                servicio VARCHAR(3) NOT NULL,
                codigo_respuesta VARCHAR(40),
                fecha_envio DATETIME,
                operador VARCHAR(40),
                respuesta_operador VARCHAR(20),
                pdu INT NOT NULL DEFAULT 0,
                credit FLOAT NOT NULL DEFAULT 0.0,
                INDEX idx_estado_celular (estado, celular),
                INDEX idx_id_campana (id_campana)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """))
        conn.execute(text("SET sql_notes = 1"))


def truncate_table(engine):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE `campana_{CAMPAIGN_ID}`"))


def drop_table(engine):
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS `campana_{CAMPAIGN_ID}`"))


_metadata = MetaData()

def campaign_table():
    return Table(
        f"campana_{CAMPAIGN_ID}", _metadata,
        Column("celular", String(18)),
        Column("id_campana", Integer),
        Column("estado", Enum("C","F","P","E","B","X","A","D")),
        Column("texto", Text),
        Column("identificacion", String(250)),
        Column("servicio", String(3)),
        Column("operador", String(40)),
        Column("pdu", Integer),
        Column("credit", Float),
        extend_existing=True,
    )


def batch_insert(engine, df: pl.DataFrame):
    table = campaign_table()
    with engine.begin() as conn:
        for chunk in df.iter_slices(INSERT_CHUNK):
            conn.execute(insert(table).values(chunk.to_dicts()))


def load_data_insert(engine, df: pl.DataFrame):
    import os, tempfile
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tsv")
    os.close(tmp_fd)
    try:
        df.write_csv(tmp_path, separator="\t", include_header=False, null_value="")
        columns_str = ", ".join(f"`{c}`" for c in df.columns)
        safe_path = tmp_path.replace("\\", "/")
        load_sql = (
            f"LOAD DATA LOCAL INFILE '{safe_path}' "
            f"INTO TABLE `campana_{CAMPAIGN_ID}` "
            f"CHARACTER SET utf8mb4 "
            f"FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '\"' "
            f"LINES TERMINATED BY '\\n' ({columns_str})"
        )
        with engine.begin() as conn:
            conn.execute(text(load_sql))
    finally:
        os.unlink(tmp_path)


# ------------------------------------------------------------------
# Run
# ------------------------------------------------------------------

def run():
    engine = make_engine()
    create_table(engine)

    print(f"\n{'Rows':>10} {'Method':<18} {'Time (s)':>10} {'Rows/s':>12}")
    print("-" * 56)

    for n in ROW_COUNTS:
        df = build_dataframe(n)
        truncate_table(engine)

        method = "LOAD DATA" if n >= THRESHOLD else "Batch INSERT"

        t0 = time.perf_counter()
        if n >= THRESHOLD:
            load_data_insert(engine, df)
        else:
            batch_insert(engine, df)
        elapsed = time.perf_counter() - t0

        rows_per_sec = int(n / elapsed)
        print(f"{n:>10,} {method:<18} {elapsed:>10.3f} {rows_per_sec:>12,}")

        truncate_table(engine)

    drop_table(engine)
    engine.dispose()
    print("\nTable dropped. Done.")


if __name__ == "__main__":
    run()
