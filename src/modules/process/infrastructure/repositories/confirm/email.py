import asyncio

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_CHUNK_ASYNC     = 10_000
_MAX_CONCURRENCY = 8


def _tbl(campaign_id: int) -> str:
    return f"mail_{campaign_id}"


class EmailConfirmRepository:
    def __init__(self, engine: AsyncEngine):
        self._engine = engine

    async def create_campaign_tables(self, campaign_ids: list[int]) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(text("SET sql_notes = 0"))
            for cid in campaign_ids:
                await conn.execute(
                    text("CALL `mail_campaings`.`create_mail_table`(:cid)"),
                    {"cid": cid},
                )
            await conn.execute(text("SET sql_notes = 1"))

    async def bulk_insert(self, campaign_id: int, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        df       = df.with_columns(pl.lit(campaign_id).alias("id_campaign"))
        cols     = df.columns
        cols_sql = ", ".join(f"`{c}`" for c in cols)
        binds    = ", ".join(f":{c}" for c in cols)
        stmt     = text(
            f"INSERT IGNORE INTO `mail_campaings`.`{_tbl(campaign_id)}` ({cols_sql}) VALUES ({binds})"
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
                await conn.execute(text("SET SESSION unique_checks=0, foreign_key_checks=0"))
                while True:
                    chunk = await queue.get()
                    if chunk is None:
                        break
                    await conn.execute(stmt, chunk.to_dicts())
                    await conn.commit()
                await conn.execute(text("SET SESSION unique_checks=1, foreign_key_checks=1"))
                await conn.commit()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(_producer())
            for _ in range(_MAX_CONCURRENCY):
                tg.create_task(_worker())

        return df.height
