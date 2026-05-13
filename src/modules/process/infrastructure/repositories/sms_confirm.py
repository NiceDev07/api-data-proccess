import asyncio

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine

from logging_config import get_logger

logger = get_logger(__name__)

_CHUNK_ASYNC     = 10_000
_MAX_CONCURRENCY = 8

# Tabla sin índices secundarios — se agregan al finalizar el bulk insert
# para evitar actualización fila a fila durante la inserción masiva.
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
        credit FLOAT NOT NULL DEFAULT 0.0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


class SmsConfirmRepository:
    def __init__(self, session: AsyncSession, async_engine: AsyncEngine):
        self._session = session
        self._engine  = async_engine

    async def create_campaign_table(self, campaign_id: int) -> None:
        await self.create_campaign_tables([campaign_id])

    async def create_campaign_tables(self, campaign_ids: list[int]) -> None:
        await self._session.execute(text("SET sql_notes = 0"))
        for campaign_id in campaign_ids:
            await self._session.execute(text(_CREATE_TABLE_SQL.format(campaign_id=campaign_id)))
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

        await self._add_indexes(campaign_id)
        return df.height

    async def _add_indexes(self, campaign_id: int) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_estado_celular "
                f"ON `campana_{campaign_id}` (estado, celular)"
            ))
            await conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_id_campana "
                f"ON `campana_{campaign_id}` (id_campana)"
            ))
