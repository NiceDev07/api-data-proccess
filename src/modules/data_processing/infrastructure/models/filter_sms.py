from sqlalchemy import Column, Integer, Text, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class FiltroSMS(Base):
    __tablename__ = "filtro_sms"

    id = Column(Integer, primary_key=True, autoincrement=True)
    termino = Column(Text)
    perfil = Column(Text)
    autorizado = Column(Integer, nullable=True)  # int(11) DEFAULT NULL
    id_autorizados = Column(String(300), nullable=True)  # varchar(300) DEFAULT NULL
