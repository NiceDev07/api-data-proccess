import asyncio

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from logging_config import get_logger

logger = get_logger(__name__)

_CHUNK_ASYNC     = 5_000
_MAX_CONCURRENCY = 4


def _tbl(campaign_id: int) -> str:
    return f'details."campaign_{campaign_id}"'


class CallBlastingConfirmRepository:
    def __init__(self, engine: AsyncEngine):
        self._engine = engine

    async def create_campaign_tables(self, campaign_ids: list[int]) -> None:
        async with self._engine.begin() as conn:
            for cid in campaign_ids:
                await conn.execute(
                    text("SELECT details.create_campaign_table(:cid)"),
                    {"cid": cid},
                )

    async def bulk_insert(self, campaign_id: int, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        cols     = df.columns
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        binds    = ", ".join(f":{c}" for c in cols)
        stmt     = text(
            f"INSERT INTO {_tbl(campaign_id)} ({cols_sql}) VALUES ({binds}) ON CONFLICT DO NOTHING"
        )

        queue: asyncio.Queue = asyncio.Queue(maxsize=_MAX_CONCURRENCY * 2)

        async def _producer() -> None:
            try:
                for chunk in df.iter_slices(n_rows=_CHUNK_ASYNC):
                    await queue.put(chunk)
            finally:
                for _ in range(_MAX_CONCURRENCY):
                    await queue.put(None)

        async def _worker() -> None:
            async with self._engine.connect() as conn:
                while True:
                    chunk = await queue.get()
                    if chunk is None:
                        break
                    await conn.execute(stmt, chunk.to_dicts())
                    await conn.commit()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(_producer())
            for _ in range(_MAX_CONCURRENCY):
                tg.create_task(_worker())

        return df.height
