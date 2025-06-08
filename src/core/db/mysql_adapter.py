from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from .interfaces.database import DatabaseSessionInterface

class SQLAlchemyAdapter(DatabaseSessionInterface):
    def __init__(self, session: Session):
        self.session = session

    def fetch_one(self, query: str, params: dict = None):
        return self.session.execute(text(query), params).fetchone()

    def fetch_all(self, query: str, params: dict = None):
        return self.session.execute(text(query), params).fetchall()

    def execute(self, query: str, params: dict = None):
        result = self.session.execute(text(query), params)
        self.session.commit()

        return result.scalar()

