import asyncio
import logging
import os
import tempfile

import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_LOAD_DATA_THRESHOLD = 10_000
_INSERT_CHUNK = 5_000



class SmsConfirmRepository:
    """
    session  → async, para create_campaign_table (DDL ligero).
    engine   → sync del lifespan, para bulk_insert (LOAD DATA / batch INSERT en threads).
    """

    def __init__(self, session: AsyncSession, engine: Engine):
        self._session = session
        self._engine = engine

    async def create_campaign_table(self, campaign_id: int) -> None:
        await self.create_campaign_tables([campaign_id])

    async def create_campaign_tables(self, campaign_ids: list[int]) -> None:
        """Crea múltiples tablas en una sola transacción — reduce round-trips de 4N a N+3."""
        await self._session.execute(text("SET sql_notes = 0"))
        for campaign_id in campaign_ids:
            await self._session.execute(
                text("CALL `telefonos_campanas`.`create_campaign_table`(:id_camp)"),
                {"id_camp": campaign_id},
            )
        await self._session.execute(text("SET sql_notes = 1"))
        await self._session.commit()

    async def bulk_insert(self, campaign_id: int, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        if df.height >= _LOAD_DATA_THRESHOLD:
            return await asyncio.to_thread(self._sync_load_data, campaign_id, df)

        return await asyncio.to_thread(self._sync_batch_insert, campaign_id, df)

    # ── sync helpers (corren en asyncio.to_thread) ────────────────────────────

    def _sync_load_data(self, campaign_id: int, df: pl.DataFrame) -> int:
        """LOAD DATA LOCAL INFILE — vía más rápida para >= 10 000 filas."""
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
            with self._engine.begin() as conn:
                conn.execute(text(load_sql))
        finally:
            os.unlink(tmp_path)
        return df.height

    def _sync_batch_insert(self, campaign_id: int, df: pl.DataFrame) -> int:
        """INSERT IGNORE multi-fila para < 10 000 filas."""
        rows = df.to_dicts()
        cols = list(rows[0].keys())
        cols_str = ", ".join(f"`{c}`" for c in cols)
        bind_str = ", ".join(f":{c}" for c in cols)
        stmt = text(
            f"INSERT IGNORE INTO `campana_{campaign_id}` ({cols_str}) VALUES ({bind_str})"
        )
        with self._engine.begin() as conn:
            conn.execute(text("SET SESSION unique_checks=0, foreign_key_checks=0"))
            for i in range(0, len(rows), _INSERT_CHUNK):
                conn.execute(stmt, rows[i : i + _INSERT_CHUNK])
            conn.execute(text("SET SESSION unique_checks=1, foreign_key_checks=1"))
        return df.height
