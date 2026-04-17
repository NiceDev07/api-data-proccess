"""
Email bulk insert performance test.
Multiplies synthetic email records to reach different row counts and measures
insertion time using both strategies (Batch INSERT and LOAD DATA LOCAL INFILE).

Inserts into mail_campaings.mail_test — safe to run without touching production data.
Creates a disposable campaign in campaigns_test for FK compliance and removes it at the end.

Usage:
    uv run python test_bulk_insert_email.py
"""
import os
import re
import sys
import tempfile
import time

import polars as pl
from sqlalchemy import (
    Column, Integer, MetaData, String, Table, Text, create_engine, insert, text
)

sys.path.insert(0, "src")
from config.settings import settings

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
TEST_TABLE   = "mail_test"
TEST_SCHEMA  = "mail_campaings"
THRESHOLD    = 50_000        # Must match EmailConfirmRepository._LOAD_DATA_THRESHOLD
INSERT_CHUNK = 5_000
ROW_COUNTS   = [100, 1_000, 10_000, 50_000, 100_000, 500_000, 1_000_000]

SYNC_DSN = re.sub(r"^mysql\+\w+://", "mysql+pymysql://", settings.DB_EMAIL)

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def make_engine():
    return create_engine(SYNC_DSN, connect_args={"local_infile": True})


def create_test_campaign(engine) -> int:
    """Insert a disposable campaign in campaigns_test and return its id."""
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO `{TEST_SCHEMA}`.`campaigns_test`
                (name_campaign, date_start, id_list, name_list,
                 iduser, username, registers, executed, openings,
                 bounces, subject, mail_responses, name_responses)
            VALUES
                ('__benchmark_test__', NOW(), 0, '__test__',
                 0, '__test__', 0, 0, 0,
                 0, '__test__', '__test__', '__test__')
        """))
        row = conn.execute(text("SELECT LAST_INSERT_ID()")).fetchone()
    return row[0]


def delete_test_campaign(engine, campaign_id: int):
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM `{TEST_SCHEMA}`.`campaigns_test` WHERE id = :id"),
            {"id": campaign_id},
        )


def build_dataframe(n_rows: int) -> pl.DataFrame:
    """Generate synthetic email rows (columns sent in the TSV for LOAD DATA)."""
    base = pl.DataFrame({
        "mail":        ["test.user@gmail.com", "otro.usuario@hotmail.com"],
        "id_client":   ["CLI-001", "CLI-002"],
        "status":      ["P", "X"],
        "body":        ["Hola, este es el cuerpo del correo de prueba 😊",
                        "Este registro está excluido"],
        "services":    ["1", "2"],
        "subject":     ["Asunto de prueba", "Asunto de prueba"],
        "name_client": ["Cliente Test", "Cliente Excluido"],
    })
    repeats = (n_rows // base.height) + 1
    return pl.concat([base] * repeats).head(n_rows)


def create_table(engine):
    with engine.begin() as conn:
        conn.execute(text("SET sql_notes = 0"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{TEST_SCHEMA}`.`{TEST_TABLE}` (
                id          INT AUTO_INCREMENT PRIMARY KEY,
                mail        VARCHAR(255) NOT NULL,
                id_campaign INT NOT NULL,
                id_client   VARCHAR(250),
                status      VARCHAR(1) NOT NULL DEFAULT 'P',
                body        TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                services    VARCHAR(3) NOT NULL,
                subject     TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                name_client VARCHAR(250),
                INDEX idx_status_mail (status, mail),
                INDEX idx_id_campaign (id_campaign)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """))
        conn.execute(text("SET sql_notes = 1"))


def truncate_table(engine):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE `{TEST_SCHEMA}`.`{TEST_TABLE}`"))


def drop_table(engine):
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS `{TEST_SCHEMA}`.`{TEST_TABLE}`"))


_metadata = MetaData()

def mail_table():
    return Table(
        TEST_TABLE,
        _metadata,
        Column("mail",        String(255),  nullable=False),
        Column("id_campaign", Integer,      nullable=False),
        Column("id_client",   String(250)),
        Column("status",      String(1),    nullable=False),
        Column("body",        Text,         nullable=False),
        Column("services",    String(3),    nullable=False),
        Column("subject",     Text),
        Column("name_client", String(250)),
        schema=TEST_SCHEMA,
        extend_existing=True,
    )


def batch_insert(engine, campaign_id: int, df: pl.DataFrame):
    rows = df.with_columns(pl.lit(campaign_id).alias("id_campaign")).to_dicts()
    table = mail_table()
    with engine.begin() as conn:
        for i in range(0, len(rows), INSERT_CHUNK):
            conn.execute(insert(table).values(rows[i : i + INSERT_CHUNK]))


def load_data_insert(engine, campaign_id: int, df: pl.DataFrame):
    columns_str = ", ".join(f"`{c}`" for c in df.columns)
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tsv")
    os.close(tmp_fd)
    try:
        df.write_csv(tmp_path, separator="\t", include_header=False, null_value="")
        safe_path = tmp_path.replace("\\", "/")
        load_sql = (
            f"LOAD DATA LOCAL INFILE '{safe_path}' "
            f"INTO TABLE `{TEST_SCHEMA}`.`{TEST_TABLE}` "
            f"CHARACTER SET utf8mb4 "
            f"FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '\"' "
            f"LINES TERMINATED BY '\\n' ({columns_str}) "
            f"SET id_campaign = {campaign_id}"
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

    campaign_id = create_test_campaign(engine)
    print(f"\nTabla destino : {TEST_SCHEMA}.{TEST_TABLE}")
    print(f"Campaign test : {campaign_id}  (se elimina al final)")
    print(f"\n{'Rows':>10} {'Method':<18} {'Time (s)':>10} {'Rows/s':>12}")
    print("-" * 56)

    try:
        for n in ROW_COUNTS:
            df = build_dataframe(n)
            truncate_table(engine)

            method = "LOAD DATA" if n >= THRESHOLD else "Batch INSERT"

            t0 = time.perf_counter()
            if n >= THRESHOLD:
                load_data_insert(engine, campaign_id, df)
            else:
                batch_insert(engine, campaign_id, df)
            elapsed = time.perf_counter() - t0

            rows_per_sec = int(n / elapsed)
            print(f"{n:>10,} {method:<18} {elapsed:>10.3f} {rows_per_sec:>12,}")

            truncate_table(engine)
    finally:
        delete_test_campaign(engine, campaign_id)
        drop_table(engine)
        engine.dispose()
        print(f"\nCampaign {campaign_id} eliminada. Tabla {TEST_TABLE} eliminada. Listo.")


if __name__ == "__main__":
    run()
