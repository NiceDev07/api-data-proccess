from sqlalchemy.orm import Session
from redis import Redis


class OperatorRepository:
    def __init__(self, db: Session, redis_client: Redis):
        self.db = db
        self.redis = redis_client
        
    def get_all_operators(self):
        return self.db.query("SELECT * FROM operators").all()