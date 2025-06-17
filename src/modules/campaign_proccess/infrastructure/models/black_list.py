from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Enum,
    ForeignKey,
    UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class BlackList(Base):
    __tablename__ = 'black_list'
    __table_args__ = (
        UniqueConstraint('source_addr', name='uq_source_addr'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_addr = Column(String(40), nullable=False)
    destination_addr = Column(String(40), nullable=False)
    validity = Column(DateTime, nullable=False)
    content = Column(Text, nullable=False)
    status = Column(Enum('A', 'D', name='status_enum'), nullable=False, default='A', comment="'A is Active, D is Disabled'")
    country_id = Column(Integer, nullable=False, default=81)

    def __repr__(self):
        return (
            f"<BlackList(id={self.id}, source_addr='{self.source_addr}', "
            f"destination_addr='{self.destination_addr}', validity={self.validity}, "
            f"status='{self.status}', country_id={self.country_id})>"
        )
