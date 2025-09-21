from fastapi import Depends
from modules._common.infrastructure.db import get_db_saem3, get_db_masivos_sms, get_db_portabilidad
from modules.data_processing.infrastructure.db.db_context import DbContext
from sqlalchemy.ext.asyncio import AsyncSession

async def get_databases(
    saem3_db: AsyncSession = Depends(get_db_saem3),
    masivos_db: AsyncSession = Depends(get_db_masivos_sms),
    portabilidad_db: AsyncSession = Depends(get_db_portabilidad),
) -> DbContext:
    return DbContext(
        saem3=saem3_db,
        masivos_sms=masivos_db,
        portabilidad_db=portabilidad_db,
    )