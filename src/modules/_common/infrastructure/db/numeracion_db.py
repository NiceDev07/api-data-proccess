from sqlalchemy import create_engine
from config.settings import settings
from sqlalchemy.orm import sessionmaker, Session

portabilidad_engine = create_engine(settings.DB_PORTABILIDAD)
PortabilidadSession = sessionmaker(autocommit=False, autoflush=False, bind=portabilidad_engine)

def get_db_portabilidad() -> Session: # type: ignore
    db = PortabilidadSession()
    try:
        yield db
    finally:
        db.close()