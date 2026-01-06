from sqlalchemy import Column, Integer, BigInteger, String
from .base import Base

class Numeracion(Base):
    __tablename__ = 'numeracion'

    id = Column(Integer, primary_key=True, autoincrement=True)
    empresa = Column(String(100), nullable=True)
    tipo_numeracion = Column(String(50), nullable=True)
    inicio = Column(BigInteger, nullable=True)
    fin = Column(BigInteger, nullable=True)
    id_operador = Column(Integer, nullable=True)
    operador = Column(String(100), nullable=True)
    carrierid = Column(Integer, nullable=True)
    codigo_ci = Column(String(10), nullable=True)
    id_pais = Column(Integer, nullable=True)
