import asyncio
import logging
import os
import re
import tempfile

import polars as pl
from sqlalchemy import Table, Column, Integer, String, Text, MetaData, insert, text, create_engine

logger = logging.getLogger(__name__)

_LOAD_DATA_THRESHOLD = 50_000
_INSERT_CHUNK = 5_000

_metadata = MetaData()

_mail_table = Table(
    "mail",
    _metadata,
    Column("mail", String(255), nullable=False),
    Column("id_campaign", Integer, nullable=False),
    Column("id_client", String(250)),
    Column("status", String(1), nullable=False),
    Column("body", Text, nullable=False),
    Column("services", String(3), nullable=False),
    Column("subject", Text),
    Column("name_client", String(250)),
    schema="mail_campaings",
    extend_existing=True,
)


def _to_sync_dsn(dsn: str) -> str:
    return re.sub(r"^mysql\+\w+://", "mysql+pymysql://", dsn)


class EmailConfirmRepository:
    def __init__(self, sync_dsn: str):
        self._sync_dsn = _to_sync_dsn(sync_dsn)

    async def assert_campaigns_exist(self, campaign_ids: list[int]) -> None:
        """Raise ValueError listing any campaign IDs missing in mail_campaings.campaigns."""
        missing = await asyncio.to_thread(self._sync_check_campaigns, campaign_ids)
        if missing:
            raise ValueError(
                f"Las siguientes campañas no existen en mail_campaings.campaigns: {missing}"
            )

    async def bulk_insert(self, campaign_id: int, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        if df.height >= _LOAD_DATA_THRESHOLD:
            return await asyncio.to_thread(self._sync_load_data, campaign_id, df)

        return await asyncio.to_thread(self._sync_batch_insert, campaign_id, df)

    # ------------------------------------------------------------------
    # Sync operations run in a thread via asyncio.to_thread.
    # ------------------------------------------------------------------

    def _sync_check_campaigns(self, campaign_ids: list[int]) -> list[int]:
        placeholders = ", ".join(str(i) for i in campaign_ids)
        engine = self._sync_engine()
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(f"SELECT id FROM `mail_campaings`.`campaigns` WHERE id IN ({placeholders})")
                ).fetchall()
        finally:
            engine.dispose()
        found = {row[0] for row in rows}
        return [cid for cid in campaign_ids if cid not in found]

    def _sync_engine(self):
        return create_engine(
            self._sync_dsn,
            connect_args={"local_infile": True},
        )

    def _sync_load_data(self, campaign_id: int, df: pl.DataFrame) -> int:
        """LOAD DATA LOCAL INFILE — fastest path for >= 50 000 rows."""
        columns_str = ", ".join(f"`{c}`" for c in df.columns)
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tsv")
        os.close(tmp_fd)
        try:
            df.write_csv(tmp_path, separator="\t", include_header=False, null_value="")
            safe_path = tmp_path.replace("\\", "/")
            load_sql = (
                f"LOAD DATA LOCAL INFILE '{safe_path}' "
                f"INTO TABLE `mail_campaings`.`mail` "
                f"CHARACTER SET utf8mb4 "
                f"FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '\"' "
                f"LINES TERMINATED BY '\\n' ({columns_str}) "
                f"SET id_campaign = {campaign_id}"
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
        rows = df.with_columns(
            pl.lit(campaign_id).alias("id_campaign")
        ).to_dicts()
        engine = self._sync_engine()
        try:
            with engine.begin() as conn:
                for i in range(0, len(rows), _INSERT_CHUNK):
                    conn.execute(insert(_mail_table).values(rows[i : i + _INSERT_CHUNK]))
        finally:
            engine.dispose()
        return df.height
