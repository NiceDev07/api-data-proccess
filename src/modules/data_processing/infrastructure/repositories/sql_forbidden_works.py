from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from modules.data_processing.domain.interfaces.forbidden_words_repository import IForbiddenWordsRepository
from modules.data_processing.infrastructure.models.filter_sms import FiltroSMS  # Assuming this is the model for forbidden words in your database

class ForbiddenWordsRepository(IForbiddenWordsRepository):
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_word_not_allowed(self):
        try:
            stmt = select(FiltroSMS.termino, FiltroSMS.id_autorizados)
            result = await self.db.execute(stmt)
            return result.all()  # devuelve lista de tuplas (termino, id_autorizados)
        except Exception as e:
            await self.db.rollback()
            print(e)
            raise ValueError("Error al obtener las palabras no permitidas") from e