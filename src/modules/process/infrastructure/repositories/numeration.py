from typing import List, Tuple
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from ..models.numeration import Numeracion
from modules.process.domain.interfaces.numeracion_repository import INumeracionRepository
from sqlalchemy.ext.asyncio import AsyncSession

class NumeracionRepository(INumeracionRepository):
    def __init__(self, db_numeracioon: AsyncSession):
        self.db_connection = db_numeracioon

    async def get_numeracion(self, country_id: int) -> List[Tuple[int, int, str]]:
        try:
            stmt = (
                select(
                    Numeracion.inicio,
                    Numeracion.fin,
                    Numeracion.operador
                )
                .where(Numeracion.id_pais == country_id)
                .order_by(Numeracion.inicio.asc())
            )
            result = await self.db_connection.execute(stmt)  # <- await aquí
            rows = result.all()

            if not rows:
                raise ValueError(f"No numeration data found for country_id {country_id}")

            return rows                                       # List[Tuple[int,int,str]]
        except Exception as e:
            await self.db_connection.rollback()               # <- con await
            raise ValueError("Error al obtener numeración") from e