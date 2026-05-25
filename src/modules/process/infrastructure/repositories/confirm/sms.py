import asyncio

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine

_CHUNK_ASYNC     = 10_000
_MAX_CONCURRENCY = 8

class SmsConfirmRepository:
    def __init__(self, session: AsyncSession, async_engine: AsyncEngine):
        self._session = session
        self._engine  = async_engine

    async def create_campaign_table(self, campaign_id: int) -> None:
        await self.create_campaign_tables([campaign_id])

    async def create_campaign_tables(self, campaign_ids: list[int]) -> None:
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

        cols     = df.columns
        cols_sql = ", ".join(f"`{c}`" for c in cols)
        binds    = ", ".join(f":{c}" for c in cols)
        stmt     = text(f"INSERT IGNORE INTO `campana_{campaign_id}` ({cols_sql}) VALUES ({binds})")

        # Queue con backpressure: el producer se bloquea si los workers no consumen
        queue: asyncio.Queue = asyncio.Queue(maxsize=_MAX_CONCURRENCY * 2)

        async def _producer() -> None:
            try:
                for chunk in df.iter_slices(n_rows=_CHUNK_ASYNC):
                    await queue.put(chunk)
            finally:
                # Poison pill por cada worker para que terminen limpiamente
                for _ in range(_MAX_CONCURRENCY):
                    await queue.put(None)

        async def _worker() -> None:
            # Conexión persistente por worker — SET SESSION una sola vez
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

        # TaskGroup cancela todos los tasks si alguno falla — evita deadlocks
        async with asyncio.TaskGroup() as tg:
            tg.create_task(_producer())
            for _ in range(_MAX_CONCURRENCY):
                tg.create_task(_worker())

        return df.height
