from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from config import settings

# Base común si compartes modelos (opcional)
# Base = declarative_base()

saem3_engine = create_engine(settings.DB_SAEM3)
forbidden_words_engine = create_engine(settings.DB_FORBIDDEN_WORDS)

Saem3SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=saem3_engine)
ForbiddenWordsSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=forbidden_words_engine)

def get_db_saem3() -> Session: # type: ignore
    db = Saem3SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db_forbidden_words() -> Session: # type: ignore
    db = ForbiddenWordsSessionLocal()
    try:
        yield db
    finally:
        db.close()