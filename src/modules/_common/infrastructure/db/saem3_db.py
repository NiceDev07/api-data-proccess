from sqlalchemy import create_engine
from config.settings import settings
from sqlalchemy.orm import sessionmaker, Session

saem3_engine = create_engine(settings.DB_SAEM3)
Saem3Session = sessionmaker(autocommit=False, autoflush=False, bind=saem3_engine)

def get_db_saem3() -> Session: # type: ignore
    db = Saem3Session()
    try:
        yield db
    finally:
        db.close()