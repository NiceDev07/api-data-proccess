from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.settings import settings

masivos_sms = create_engine(settings.DB_MASIVOS_SMS)
MasivosSMSSession = sessionmaker(autocommit=False, autoflush=False, bind=masivos_sms)

def get_db_masivos_sms(): # type: ignore
    db = MasivosSMSSession()
    try:
        yield db
    finally:
        db.close()