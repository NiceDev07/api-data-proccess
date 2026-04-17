import asyncio
import logging
import os
import tempfile

import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_LOAD_DATA_THRESHOLD = 10_000
_INSERT_CHUNK = 5_000


def _tbl(campaign_id: int) -> str:
    """Nombre dinámico de tabla: mail_{id} — igual que el SP."""
    return f"mail_{campaign_id}"


class EmailConfirmRepository:
    """
    Recibe el sync Engine del lifespan vía Depends().
    No crea ni gestiona pools propios.
    """

    def __init__(self, engine: Engine):
        self._engine = engine

    async def assert_campaigns_exist(self, campaign_ids: list[int]) -> None:
        missing = await asyncio.to_thread(self._sync_check_campaigns, campaign_ids)
        if missing:
            raise ValueError(
                f"Las siguientes campañas no existen en mail_campaings.campaigns: {missing}"
            )

    async def bulk_insert(self, campaign_id: int, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        # 1. Crear tabla dedicada vía SP (idempotente si ya existe)
        await asyncio.to_thread(self._sync_create_table, campaign_id)

        # 2. LOAD DATA para >= threshold; batch INSERT para el resto
        if df.height >= _LOAD_DATA_THRESHOLD:
            return await asyncio.to_thread(self._sync_load_data, campaign_id, df)

        return await asyncio.to_thread(self._sync_batch_insert, campaign_id, df)

    # ── sync helpers (corren en asyncio.to_thread) ────────────────────────────

    def _sync_check_campaigns(self, campaign_ids: list[int]) -> list[int]:
        placeholders = ", ".join(str(i) for i in campaign_ids)
        with self._engine.connect() as conn:
            rows = conn.execute(
                text(f"SELECT id FROM `mail_campaings`.`campaigns` WHERE id IN ({placeholders})")
            ).fetchall()
        found = {row[0] for row in rows}
        return [cid for cid in campaign_ids if cid not in found]

    def _sync_create_table(self, campaign_id: int) -> None:
        """Llama al SP que crea la tabla dedicada mail_{id}."""
        with self._engine.begin() as conn:
            conn.execute(
                text("CALL `mail_campaings`.`create_mail_table`(:cid)"),
                {"cid": campaign_id},
            )

    def _sync_load_data(self, campaign_id: int, df: pl.DataFrame) -> int:
        """LOAD DATA LOCAL INFILE — vía más rápida para >= 10 000 filas."""
        tbl = _tbl(campaign_id)
        df = df.with_columns(pl.lit(campaign_id).alias("id_campaign"))
        columns_str = ", ".join(f"`{c}`" for c in df.columns)
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tsv")
        os.close(tmp_fd)
        try:
            df.write_csv(tmp_path, separator="\t", include_header=False, null_value="")
            safe_path = tmp_path.replace("\\", "/")
            load_sql = (
                f"LOAD DATA LOCAL INFILE '{safe_path}' "
                f"INTO TABLE `mail_campaings`.`{tbl}` "
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
        tbl = _tbl(campaign_id)
        rows = df.with_columns(pl.lit(campaign_id).alias("id_campaign")).to_dicts()
        cols = list(rows[0].keys())
        cols_str = ", ".join(f"`{c}`" for c in cols)
        bind_str = ", ".join(f":{c}" for c in cols)
        stmt = text(
            f"INSERT IGNORE INTO `mail_campaings`.`{tbl}` ({cols_str}) VALUES ({bind_str})"
        )
        with self._engine.begin() as conn:
            conn.execute(text("SET SESSION unique_checks=0, foreign_key_checks=0"))
            for i in range(0, len(rows), _INSERT_CHUNK):
                conn.execute(stmt, rows[i : i + _INSERT_CHUNK])
            conn.execute(text("SET SESSION unique_checks=1, foreign_key_checks=1"))
        return df.height
