from sqlalchemy.orm import Session
from redis import Redis

class UnauthorizedWordsRepository:
    def __init__(self, db: Session, redis_client: Redis):
        self.db = db
        self.redis = redis_client

    def get_all_unauthorized_words(self):
        return self.db.query("SELECT * FROM unauthorized_words").all()