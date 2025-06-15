from sqlalchemy.orm import Session
from modules.campaign_proccess.domain.interfaces.forbidden_words_repository import IForbiddenWordsRepository

class ForbiddenWordsRepository(IForbiddenWordsRepository):
    def __init__(self, db: Session):
        self.db = db

    def get_all_unauthorized_words(self):
        return self.db.query("SELECT * FROM unauthorized_words").all()