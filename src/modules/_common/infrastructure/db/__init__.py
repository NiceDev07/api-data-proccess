from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from fastapi import Request

def get_session_factory(db_name: str):
    async def _get_session(request: Request):
        engine = request.app.state.engines.get(db_name, None)
        SessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
        async with SessionLocal() as session:
            yield session
    return _get_session

get_db_saem3 = get_session_factory("saem3")
get_db_portabilidad = get_session_factory("portabilidad")
get_db_masivos_sms = get_session_factory("masivos_sms")
get_db_telefonos_campanas = get_session_factory("telefonos_campanas")
get_db_email = get_session_factory("email")