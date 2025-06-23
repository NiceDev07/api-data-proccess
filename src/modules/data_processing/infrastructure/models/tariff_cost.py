from sqlalchemy import Column, Integer, String, DECIMAL
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CostTable(Base):
    __tablename__ = 'w48fa_tel_cost'

    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(10), nullable=False)
    prefix = Column(String(10), nullable=False)
    operator = Column(String(50), nullable=False)
    sms = Column(DECIMAL(10, 4), nullable=False)
    cb_standard = Column(DECIMAL(10, 4), nullable=False)
    cb_custom = Column(DECIMAL(10, 4), nullable=False)
    email = Column(DECIMAL(10, 4), nullable=False)
    initial = Column(DECIMAL(10, 4), nullable=False)
    incremental = Column(DECIMAL(10, 4), nullable=False)
    tariff_id = Column(Integer, nullable=False)
    country_id = Column(Integer, nullable=False)
