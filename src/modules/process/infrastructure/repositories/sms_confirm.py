import asyncio
import logging
import os
import re
import tempfile

import polars as pl
from sqlalchemy import Table, Column, Integer, String, Text, Float, Enum, MetaData, insert, text, create_engine
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Below this threshold use multi-row VALUES INSERT; at or above use LOAD DATA LOCAL INFILE
_LOAD_DATA_THRESHOLD = 50_000
# Rows per INSERT ... VALUES (...) statement  — keeps parameter count well under MySQL's 65 535 limit
_INSERT_CHUNK = 5_000

_metadata = MetaData()

_CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS `campana_{campaign_id}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        celular VARCHAR(18) NOT NULL,
        id_campana INT NOT NULL DEFAULT '{campaign_id}',
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def _campaign_table(campaign_id: int) -> Table:
    return Table(
        f"campana_{campaign_id}",
        _metadata,
        Column("celular", String(18), nullable=False),
        Column("id_campana", Integer, nullable=False),
        Column("estado", Enum("C", "F", "P", "E", "B", "X", "A", "D"), nullable=False),
        Column("texto", Text, nullable=False),
        Column("identificacion", String(250), nullable=False),
        Column("servicio", String(3), nullable=False),
        Column("operador", String(40)),
        Column("pdu", Integer, nullable=False),
        Column("credit", Float, nullable=False),
        extend_existing=True,
    )


def _to_sync_dsn(dsn: str) -> str:
    """Convert an async DSN to its sync pymysql counterpart for use in a thread."""
    return re.sub(r"^mysql\+\w+://", "mysql+pymysql://", dsn)


class SmsConfirmRepository:
    def __init__(self, session: AsyncSession, sync_dsn: str):
        self._session = session
        self._sync_dsn = _to_sync_dsn(sync_dsn)

    async def create_campaign_table(self, campaign_id: int) -> None:
        # sql_notes=0 suppresses the "table already exists" warning from asyncmy
        await self._session.execute(text("SET sql_notes = 0"))
        await self._session.execute(text(_CREATE_TABLE_SQL.format(campaign_id=campaign_id)))
        await self._session.execute(text("SET sql_notes = 1"))
        await self._session.commit()

    async def bulk_insert(self, campaign_id: int, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        if df.height >= _LOAD_DATA_THRESHOLD:
            return await asyncio.to_thread(
                self._sync_load_data, campaign_id, df
            )

        return await asyncio.to_thread(
            self._sync_batch_insert, campaign_id, df
        )

    # ------------------------------------------------------------------
    # All sync operations run in a thread via asyncio.to_thread.
    # asyncmy (async driver) has issues with both LOAD DATA and bulk INSERT;
    # mysqlconnector (sync) handles both correctly.
    # ------------------------------------------------------------------

    def _sync_engine(self):
        return create_engine(
            self._sync_dsn,
            connect_args={"local_infile": True},
        )

    def _sync_load_data(self, campaign_id: int, df: pl.DataFrame) -> int:
        """LOAD DATA LOCAL INFILE — fastest path for >= 50 000 rows."""
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tsv")
        os.close(tmp_fd)
        try:
            columns_str = ", ".join(f"`{c}`" for c in df.columns)
            df.write_csv(tmp_path, separator="\t", include_header=False, null_value="")
            safe_path = tmp_path.replace("\\", "/")
            load_sql = (
                f"LOAD DATA LOCAL INFILE '{safe_path}' "
                f"INTO TABLE `campana_{campaign_id}` "
                f"CHARACTER SET utf8mb4 "
                f"FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '\"' "
                f"LINES TERMINATED BY '\\n' ({columns_str})"
            )
            engine = self._sync_engine()
            try:
                with engine.begin() as conn:
                    conn.execute(text(load_sql))
            finally:
                engine.dispose()
        finally:
            os.unlink(tmp_path)
        return df.height

    def _sync_batch_insert(self, campaign_id: int, df: pl.DataFrame) -> int:
        """Multi-row VALUES INSERT — reliable path for < 50 000 rows."""
        table = _campaign_table(campaign_id)
        engine = self._sync_engine()
        try:
            with engine.begin() as conn:
                for chunk in df.iter_slices(_INSERT_CHUNK):
                    conn.execute(insert(table).values(chunk.to_dicts()))
        finally:
            engine.dispose()
        return df.height
