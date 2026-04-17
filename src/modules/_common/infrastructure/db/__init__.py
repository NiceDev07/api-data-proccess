from fastapi import Request
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker


def get_session_factory(db_name: str):
    """Async session por request — inyectada vía Depends()."""
    async def _get_session(request: Request):
        engine = request.app.state.engines.get(db_name)
        SessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
        async with SessionLocal() as session:
            yield session
    return _get_session


def get_sync_engine_factory(db_name: str):
    """Sync engine compartido del lifespan — inyectado vía Depends()."""
    def _get_engine(request: Request) -> Engine:
        return request.app.state.sync_engines[db_name]
    return _get_engine


get_db_saem3               = get_session_factory("saem3")
get_db_portabilidad        = get_session_factory("portabilidad")
get_db_masivos_sms         = get_session_factory("masivos_sms")
get_db_telefonos_campanas  = get_session_factory("telefonos_campanas")
get_db_email               = get_session_factory("email")

get_sync_engine_campanas   = get_sync_engine_factory("telefonos_campanas")
get_sync_engine_email      = get_sync_engine_factory("email")
