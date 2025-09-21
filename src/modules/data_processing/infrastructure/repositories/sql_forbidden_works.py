from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from modules.data_processing.domain.interfaces.forbidden_words_repository import IForbiddenWordsRepository
from modules.data_processing.infrastructure.models.filter_sms import FiltroSMS  # Assuming this is the model for forbidden words in your database

class ForbiddenWordsRepository(IForbiddenWordsRepository):
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_word_not_allowed(self):
        try: 
            return await self.db.query(FiltroSMS.termino, FiltroSMS.id_autorizados).all()
        except Exception as e:
            self.db.rollback()
            raise ValueError("Error al obtener las palabras no permitidas") from e